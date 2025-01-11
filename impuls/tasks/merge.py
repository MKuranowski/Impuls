# © Copyright 2022-2025 Mikołaj Kuranowski
# SPDX-License-Identifier: GPL-3.0-or-later

import os
import shutil
from contextlib import contextmanager
from dataclasses import dataclass
from math import inf
from operator import itemgetter
from pathlib import Path
from tempfile import mkstemp
from typing import Generator, Iterable, NamedTuple, Type

from ..db import DBConnection
from ..model import FeedInfo, Route, Stop
from ..pipeline import Pipeline
from ..task import Task, TaskRuntime
from ..tools.geo import earth_distance_m
from ..tools.strings import find_non_conflicting_id
from ..tools.types import Self, all_non_none


@dataclass(frozen=True)
class DatabaseToMerge:
    """DatabaseToMerge represents a database to-be-merged with the runtime database."""

    resource_name: str
    """Name of the resource with the Impuls DB to be merged."""

    prefix: str
    """Prefix to add before IDs of copied entities."""

    pre_merge_pipeline: Pipeline | None = None
    """Pipeline to run just before merging. This pipeline runs on a temporary
    copy of the database resource - any changes are not persistent across runs.
    """


@dataclass(frozen=True)
class RouteHash:
    """RouteHash represents all attributes of a :py:class:`~impuls.model.Route` required
    to conclude that two routes should be merged.
    """

    id: str
    agency_id: str
    short_name: str
    type: Route.Type
    color: str

    @classmethod
    def of(cls: Type[Self], r: Route) -> Self:
        return cls(
            id=r.id,
            agency_id=r.agency_id,
            short_name=r.short_name,
            type=r.type,
            color=r.color,
        )


@dataclass(frozen=True)
class StopHash:
    """StopHash represents all attributes of a :py:class:`~impuls.model.Stop`, excluding the
    latitude and longitude, required to conclude that two stops should be merged.
    """

    id: str
    name: str
    code: str
    zone_id: str
    location_type: Stop.LocationType
    parent_station: str
    wheelchair_boarding: bool | None
    platform_code: str

    @classmethod
    def of(cls: Type[Self], s: Stop) -> Self:
        return cls(
            id=s.id,
            name=s.name,
            code=s.code,
            zone_id=s.zone_id,
            location_type=s.location_type,
            parent_station=s.parent_station,
            wheelchair_boarding=s.wheelchair_boarding,
            platform_code=s.platform_code,
        )


class ConflictResolution(NamedTuple):
    """ConflictResolution describes how IDs from an incoming DB need to be changed
    to avoid merge conflicts. Not all entities will have an entry in the :py:attr:`ids_to_change`
    list - such entities don't need to be merged, as an entity in the runtime database already
    contains a similar-enough object.
    """

    ids_to_change: list[tuple[str, str]]
    """(new_id, old_id) pairs of ids that need to be changed"""

    total: int
    merged: int


class Merge(Task):
    """Merge tasks inserts data from provided impuls databases into the current one.

    The user must ensure that ``f"{db_to_merge.prefix}{separator}{id}"`` generates unique
    ids across all data. This especially applies if the runtime database already has data.

    This tasks also performs merging of some entity types, provided they have the same ID:

    * :py:class:`~impuls.model.Agency` and :py:class:`~impuls.model.Attribution` instances
      are always merged - attributes of the first encountered instance are kept.
    * :py:class:`~impuls.model.Calendar`, :py:class:`~impuls.model.CalendarException`,
      :py:class:`~impuls.model.FareAttribute`, :py:class:`~impuls.model.FareRule`,
      :py:class:`~impuls.model.ShapePoint`, :py:class:`~impuls.model.Trip`,
      :py:class:`~impuls.model.StopTime`, :py:class:`~impuls.model.Frequency` and
      :py:class:`~impuls.model.Transfer` instances are never merged - incoming ids are always
      prefixed by the :py:attr:`DatabaseToMerge.prefix` and :py:attr:`separator`.
    * :py:class:`~impuls.model.Route` instances are merged if they have the same
      :py:attr:`~impuls.model.Route.agency_id`, :py:attr:`~impuls.model.Route.short_name`,
      :py:attr:`~impuls.model.Route.type` and :py:attr:`~impuls.model.Route.color`.
      Other attributes of the first encountered instance are kept. If the comparison attributes
      do not match, the incoming id will have a numeric suffix added.
    * :py:class:`~impuls.model.Stop` instances are merged if they have the same
      :py:attr:`~impuls.model.Stop.name`, :py:attr:`~impuls.model.Stop.code`,
      :py:attr:`~impuls.model.Stop.zone_id`, :py:attr:`~impuls.model.Stop.location_type`,
      :py:attr:`~impuls.model.Stop.parent_station`,
      :py:attr:`~impuls.model.Stop.wheelchair_boarding`,
      :py:attr:`~impuls.model.Stop.platform_code` and are within
      :py:attr:`distance_between_similar_stops_m` (default 10 meters) of each other.
      Other attributes of the first encountered instance are kept. If the comparison attributes
      do not match, the incoming id will have a numeric suffix added.
    * :py:class:`~impuls.model.Translation` merging depends on the selector:

      * all ``feed_info`` translations are completely ignored, due to the too complex logic
        of :py:class:`~impuls.model.FeedInfo` merging;
      * :py:attr:`~impuls.model.Translation.field_value` based translations are always merged -
        attributes of the first encountered instance are kept;
      * :py:attr:`~impuls.model.Translation.record_id` based ``agency`` and ``attributions``
        translations are always merged - attributes of the first encountered instance are kept;
      * :py:attr:`~impuls.model.Translation.record_id` based ``stops`` and ``routes`` translations
        are merged - attributes of the first encountered instances are kept). Any id changes caused
        by :py:class:`~impuls.model.Stop` and :py:class:`~impuls.model.Route` merging also apply
        to the :py:attr:`~impuls.model.Translation.record_id`;
      * :py:attr:`~impuls.model.Translation.record_id` based ``trips`` and ``stop_times``
        translations are never merged - incoming :py:attr:`~impuls.model.Translation.record_id`
        is always prefixed by the :py:attr:`DatabaseToMerge.prefix` and :py:attr:`separator`.

    * :py:class:`~impuls.model.FeedInfo` is treated specially:

      * If it exists in the current (runtime) database, it is kept as-is,
        and any other instances are ignored.
      * If **all** to-merge databases have a :py:class:`~impuls.model.FeedInfo` object, then the
        attributes of the first encountered one are kept, except that:

        * feed_start_date is set to the min of all encountered feed_start_date
        * feed_end_date is set to the max of all encountered feed_end_date
        * feed_version is set to all encountered feed_versions, separated with
          :py:attr:`feed_version_separator`

      * Otherwise, no :py:class:`~impuls.model.FeedInfo` object will be created.

      The first case is meant for merging on smaller, helper datasets to an already-loaded
      major database. The second case serves merging versioned datasets, with the last case
      preventing any inconsistencies in the :py:class:`~impuls.model.FeedInfo` object.

    * :py:class:`~impuls.model.ExtraTableRow` instances are never merged; the rows
      are simply copied over. Use the :py:attr:`~DatabaseToMerge.pre_merge_pipeline`
      to adjust any values or the sort order.
    * :py:attr:`Stop.zone_id <impuls.model.Stop.zone_id>` is left untouched - effectively
      merging zones across datasets.
    * :py:attr:`Trip.block_id <impuls.model.Trip.block_id>` and
      :py:attr:`Trip.shape_id <impuls.model.Trip.shape_id>` are prefixed with
      :py:attr:`DatabaseToMerge.prefix` - effectively never merging blocks or shapes across
      datasets.
    """

    databases_to_merge: list[DatabaseToMerge]
    """List of databases to merge into the runtime DB."""

    separator: str
    """Separator inserted between :py:attr:`DatabaseToMerge.prefix` and entity IDs.
    Defaults to ``:``.
    """

    feed_version_separator: str
    """Separator for :py:attr:`FeedInfo.version <impuls.model.FeedInfo.version>` when
    a new :py:class:`~impuls.model.FeedInfo` is created based on the incoming databases.
    Defaults to ``/``.
    """

    distance_between_similar_stops_m: float
    """How close should 2 stops with the same :py:class:`StopHash` be in order to be merged.
    Defaults to 10 meters.
    """

    _known_routes: dict[RouteHash, str]
    _used_route_ids: set[str]
    _known_stops: dict[StopHash, list[Stop]]
    _used_stop_ids: set[str]
    _feed_infos: list[FeedInfo | None] | None
    """None if FeedInfo should not be collected, otherwise list of FeedInfos from merged dbs"""

    def __init__(
        self,
        databases_to_merge: list[DatabaseToMerge],
        separator: str = ":",
        feed_version_separator: str = "/",
        distance_between_similar_stops_m: float = 10.0,
    ) -> None:
        super().__init__()
        self.databases_to_merge = databases_to_merge
        self.separator = separator
        self.feed_version_separator = feed_version_separator
        self.distance_between_similar_stops_m = distance_between_similar_stops_m

        # State
        self._known_routes = {}
        self._used_route_ids = set()
        self._known_stops = {}
        self._used_stop_ids = set()
        self._feed_infos = None

    def _clear_state(self) -> None:
        self._known_routes.clear()
        self._used_route_ids.clear()
        self._known_stops.clear()
        self._used_stop_ids.clear()
        self._feed_infos = None

    def execute(self, r: TaskRuntime) -> None:
        self._clear_state()

        self.logger.info("Collecting data about existing routes and stops")
        self.initialize_known_objects(r.db)

        for i, db_to_merge in enumerate(self.databases_to_merge, start=1):
            self.logger.info(
                "Merging %s (%d/%d)",
                db_to_merge.prefix,
                i,
                len(self.databases_to_merge),
            )

            db_to_merge_path = str(r.resources[db_to_merge.resource_name].stored_at)
            self.merge(r.db, db_to_merge_path, db_to_merge.prefix, db_to_merge.pre_merge_pipeline)

        self.logger.info("Resolving FeedInfo")
        self.insert_feed_info(r.db)

    def initialize_known_objects(self, db: DBConnection) -> None:
        self.initialize_known_routes(db)
        self.initialize_known_stops(db)
        self.initialize_known_feed_info(db)

    def initialize_known_routes(self, db: DBConnection) -> None:
        self._known_routes.clear()
        self._used_route_ids.clear()
        for route in db.retrieve_all(Route):
            self._used_route_ids.add(route.id)
            self._known_routes[RouteHash.of(route)] = route.id

    def initialize_known_stops(self, db: DBConnection) -> None:
        self._known_stops.clear()
        self._used_stop_ids.clear()
        for stop in db.retrieve_all(Stop):
            self._used_stop_ids.add(stop.id)
            self._known_stops[StopHash.of(stop)] = [stop]

    def initialize_known_feed_info(self, db: DBConnection) -> None:
        feed_info_count = db.count(FeedInfo)
        self._feed_infos = None if feed_info_count > 0 else []

    def merge(
        self,
        db: DBConnection,
        incoming_path: str,
        incoming_prefix: str,
        pre_merge_pipeline: Pipeline | None = None,
    ) -> None:
        # To copy objects from the incoming db, its prefix needs to be to the id columns.
        # This means that the incoming db will be mutated. To prevent multiple mutations
        # if the db is re-used across runs, it needs to be copied into a temporary file.
        with temp_db_file(incoming_path, incoming_prefix) as incoming_mut_path:
            self.run_pre_merge_pipeline(incoming_mut_path, pre_merge_pipeline)
            with attached(db, incoming_mut_path), db.transaction():
                self.merge_with_attached(db, incoming_prefix)

    @staticmethod
    def run_pre_merge_pipeline(on: str, pipeline: Pipeline | None) -> None:
        if pipeline:
            pipeline.db_path = Path(on)
            pipeline.run_on_existing_db = True
            pipeline.run()

    def merge_with_attached(self, db: DBConnection, incoming_prefix: str) -> None:
        self.merge_agencies(db)
        self.merge_attributions(db)
        self.merge_routes(db)
        self.merge_stops(db)
        self.merge_calendars(db, incoming_prefix)
        self.merge_calendar_exceptions(db)
        self.merge_fares(db, incoming_prefix)
        self.merge_shapes(db, incoming_prefix)
        self.merge_trips(db, incoming_prefix)
        self.merge_stop_times(db)
        self.merge_frequencies(db)
        self.merge_transfers(db)
        self.merge_translations(db)
        self.merge_extra_table_rows(db)
        self.collect_incoming_feed_info(db)

    def merge_agencies(self, db: DBConnection) -> None:
        self.logger.debug("Joining Agencies")
        db.raw_execute("INSERT OR IGNORE INTO agencies SELECT * FROM incoming.agencies")

    def merge_attributions(self, db: DBConnection) -> None:
        self.logger.debug("Merging Attributions")
        db.raw_execute("INSERT OR IGNORE INTO attributions SELECT * FROM incoming.attributions")

    def merge_routes(self, db: DBConnection) -> None:
        self.logger.debug("Resolving Routes to merge")
        resolution = self.resolve_route_conflicts(db)
        self.logger.info("Merged %d out of %d Routes", resolution.merged, resolution.total)

        self.logger.debug("Joining Routes")
        db.raw_execute_many(
            "UPDATE incoming.routes SET route_id = ? WHERE route_id = ?",
            resolution.ids_to_change,
        )
        db.raw_execute_many(
            "UPDATE incoming.translations SET record_id = ? WHERE "
            "TABLE_NAME = 'routes' AND record_id = ?",
            resolution.ids_to_change,
        )
        # At this point, only to-be-merged routes have the same ids - it's safe to ignore conflicts
        db.raw_execute("INSERT OR IGNORE INTO routes SELECT * FROM incoming.routes")

    def resolve_route_conflicts(self, db: DBConnection) -> ConflictResolution:
        incoming_ids_to_change: list[tuple[str, str]] = []
        total = 0
        merged = 0

        for incoming_route in db.typed_out_execute("SELECT * FROM incoming.routes", Route):
            total += 1
            hash = RouteHash.of(incoming_route)
            new_id = self._known_routes.get(hash)

            if new_id is None:
                new_id = find_non_conflicting_id(
                    self._used_route_ids,
                    incoming_route.id,
                    self.separator,
                )
                self._used_route_ids.add(new_id)
                self._known_routes[hash] = new_id
            else:
                merged += 1

            if incoming_route.id != new_id:
                # Tuple order is (new_id, old_id) for substitution in
                # SET route_id = ? WHERE route_id = ?
                incoming_ids_to_change.append((new_id, incoming_route.id))

        return ConflictResolution(incoming_ids_to_change, total, merged)

    def merge_stops(self, db: DBConnection) -> None:
        self.logger.debug("Resolving Stops to merge")
        resolution = self.resolve_stop_conflicts(db)
        self.logger.info("Merged %d out of %d Stops", resolution.merged, resolution.total)

        self.logger.debug("Joining Stops")
        db.raw_execute_many(
            "UPDATE incoming.stops SET stop_id = ? WHERE stop_id = ?",
            resolution.ids_to_change,
        )
        db.raw_execute_many(
            "UPDATE incoming.translations SET record_id = ? WHERE "
            "TABLE_NAME = 'stops' AND record_id = ?",
            resolution.ids_to_change,
        )
        # At this point, only to-be-merged stops have the same ids - it's safe to ignore conflicts
        db.raw_execute("INSERT OR IGNORE INTO stops SELECT * FROM incoming.stops")

    def resolve_stop_conflicts(self, db: DBConnection) -> ConflictResolution:
        incoming_ids_to_change: list[tuple[str, str]] = []
        total = 0
        merged = 0

        for incoming_stop in db.typed_out_execute("SELECT * FROM incoming.stops", Stop):
            total += 1
            hash = StopHash.of(incoming_stop)
            similar_stop = pick_closest_stop(
                incoming_stop,
                self._known_stops.get(hash, []),
                self.distance_between_similar_stops_m,
            )

            if similar_stop is not None:
                merged += 1
                new_id = similar_stop.id

            else:
                new_id = find_non_conflicting_id(
                    self._used_stop_ids,
                    incoming_stop.id,
                    self.separator,
                )
                self._used_stop_ids.add(new_id)
                self._known_stops.setdefault(hash, []).append(incoming_stop)

            if incoming_stop.id != new_id:
                # Tuple order is (new_id, old_id) for substitution in
                # SET route_id = ? WHERE route_id = ?
                incoming_ids_to_change.append((new_id, incoming_stop.id))

                # The stop object will be remembered in `known_stops`, and must
                # be remembered with the new_id - otherwise merging won't work properly -
                # pick_closest_stop returns the Stop instance, not StopHash. This is not necessary
                # for routes, where all merging happens with RouteHash objects.
                incoming_stop.id = new_id

        return ConflictResolution(incoming_ids_to_change, total, merged)

    def merge_calendars(self, db: DBConnection, incoming_prefix: str) -> None:
        self.logger.debug("Joining Calendars")

        db.raw_execute(
            "UPDATE incoming.calendars SET calendar_id = ? || ? || calendar_id",
            (incoming_prefix, self.separator),
        )
        db.raw_execute("INSERT OR ABORT INTO calendars SELECT * FROM incoming.calendars")

    def merge_calendar_exceptions(self, db: DBConnection) -> None:
        self.logger.debug("Joining CalendarExceptions")

        # NOTE: merge_calendars should have updated the calendar_id
        db.raw_execute(
            "INSERT OR ABORT INTO calendar_exceptions SELECT * FROM incoming.calendar_exceptions",
        )

    def merge_fares(self, db: DBConnection, incoming_prefix: str) -> None:
        self.logger.debug("Joining FareAttributes")

        db.raw_execute(
            "UPDATE incoming.fare_attributes SET fare_id = ? || ? || fare_id",
            (incoming_prefix, self.separator),
        )
        db.raw_execute(
            "INSERT OR ABORT INTO fare_attributes SELECT * FROM incoming.fare_attributes"
        )

        self.logger.debug("Joining Fare Rules")
        # NOTE: merge_routes should have updated route_id
        # NOTE: to avoid collisions, fare_rule_id must not be copied -
        #       SQLite will automatically generate new ones (thanks to INTEGER PRIMARY KEY)
        columns = "route_id, origin_id, destination_id, contains_id"
        db.raw_execute(
            f"INSERT OR ABORT INTO fare_rules ({columns}) "
            f"SELECT {columns} FROM incoming.fare_rules"
        )

    def merge_shapes(self, db: DBConnection, incoming_prefix: str) -> None:
        self.logger.debug("Joining Shapes")

        db.raw_execute(
            "UPDATE incoming.shapes SET shape_id = ? || ? || shape_id",
            (incoming_prefix, self.separator),
        )
        db.raw_execute("INSERT OR ABORT INTO shapes SELECT * FROM incoming.shapes")

        self.logger.debug("Joining Shapes Points")
        db.raw_execute("INSERT OR ABORT INTO shape_points SELECT * FROM incoming.shape_points")

    def merge_trips(self, db: DBConnection, incoming_prefix: str) -> None:
        self.logger.debug("Joining Trips")

        # NOTE: merge_routes should have updated the route_id
        # NOTE: merge_calendars should have updated the calendar_id
        # NOTE: merge_shapes should have updated the shape_id
        db.raw_execute(
            "UPDATE incoming.trips SET trip_id = ? || ? || trip_id",
            (incoming_prefix, self.separator),
        )
        db.raw_execute(
            "UPDATE incoming.trips SET block_id = ? || ? || block_id WHERE block_id IS NOT NULL",
            (incoming_prefix, self.separator),
        )
        db.raw_execute(
            "UPDATE incoming.translations SET record_id = ? || ? || record_id "
            "WHERE record_id != '' AND table_name IN ('trips', 'stop_times')",
            (incoming_prefix, self.separator),
        )
        db.raw_execute("INSERT OR ABORT INTO trips SELECT * FROM incoming.trips")

    def merge_stop_times(self, db: DBConnection) -> None:
        self.logger.debug("Joining StopTimes")

        # NOTE: merge_stops should have updated the stop_id
        # NOTE: merge_trips should have updated the trip_id
        db.raw_execute("INSERT OR ABORT INTO stop_times SELECT * FROM incoming.stop_times")

    def merge_frequencies(self, db: DBConnection) -> None:
        self.logger.debug("Joining Frequencies")

        # NOTE: merge_trips should have updated the trip_id
        db.raw_execute("INSERT OR ABORT INTO frequencies SELECT * FROM incoming.frequencies")

    def merge_transfers(self, db: DBConnection) -> None:
        self.logger.debug("Joining Transfers")

        # NOTE: merge_routes should have updated from_route_id & to_route_id
        # NOTE: merge_stops should have updated from_stop_id & to_stop_id
        # NOTE: merge_trips should have updated from_trip_id & to_trip_id
        # NOTE: to avoid collisions, transfer_id must not be copied -
        #       SQLite will automatically generate new ones (thanks to INTEGER PRIMARY KEY)
        columns = (
            "from_stop_id, to_stop_id, from_route_id, to_route_id, from_trip_id, "
            "to_trip_id, transfer_type, min_transfer_time"
        )
        db.raw_execute(
            f"INSERT OR ABORT INTO transfers ({columns}) SELECT {columns} FROM incoming.transfers"
        )

    def merge_translations(self, db: DBConnection) -> None:
        self.logger.debug("Joining Translations")

        # NOTE: merge_routes should have updated record_id of route translations
        # NOTE: merge_stops should have updated record_id of stops translations
        # NOTE: merge_trips should have updated record_id of trip and stop_time translations

        db.raw_execute("DELETE FROM incoming.translations WHERE table_name = 'feed_info'")

        # NOTE: to avoid collisions, translation_id must not be copied -
        #       SQLite will automatically generate new ones (thanks to INTEGER PRIMARY KEY)
        columns = (
            "table_name, field_name, language, translation, record_id, record_sub_id, field_value"
        )
        db.raw_execute(
            f"INSERT OR IGNORE INTO translations ({columns}) "
            f"SELECT {columns} FROM incoming.translations"
        )

    def merge_extra_table_rows(self, db: DBConnection) -> None:
        self.logger.debug("Joining ExtraTableRows")

        # NOTE: to avoid collisions, extra_table_row_id must not be copied -
        #       SQLite will automatically generate new ones (thanks to INTEGER PRIMARY KEY)
        columns = "table_name, fields_json, row_sort_order"
        db.raw_execute(
            f"INSERT OR ABORT INTO extra_table_rows ({columns}) "
            f"SELECT {columns} FROM incoming.extra_table_rows"
        )

    def collect_incoming_feed_info(self, db: DBConnection) -> None:
        self.logger.debug("Collecting FeedInfo")

        if self._feed_infos is not None:
            feed_info = db.typed_out_execute("SELECT * FROM incoming.feed_info", FeedInfo).one()
            self._feed_infos.append(feed_info)

    def insert_feed_info(self, db: DBConnection) -> None:
        # Shallow copy - pyright can't narrow the type to list[FeedInfo] after a type guard
        # when operating directly on the class attribute.
        feed_infos = self._feed_infos

        if not feed_infos:
            return  # DB had FeedInfo before merging - keep it

        if not all_non_none(feed_infos):
            return  # Not all incoming feeds had FeedInfo - don't create one

        # All incoming dbs had FeedInfo - take attributes from the first one,
        # with version combined from all feed infos.
        new_fi: FeedInfo = feed_infos[0]
        new_fi.version = self.feed_version_separator.join(fi.version for fi in feed_infos)
        db.typed_in_execute("INSERT OR REPLACE INTO :table VALUES :vals", new_fi)


@contextmanager
def attached(db: DBConnection, path_to_incoming: str) -> Generator[None, None, None]:
    """Attaches a database from the provided path as ``incoming``.
    Detaches that database on exit.
    """
    db.raw_execute("ATTACH DATABASE ? as incoming", (path_to_incoming,))
    try:
        yield
    finally:
        db.raw_execute("DETACH DATABASE incoming")


@contextmanager
def temp_db_file(db_path: str, db_prefix: str) -> Generator[str, None, None]:
    """Creates a temporary copy of a database, so that it can be mutated
    without the changes being visible in ``db_path``.

    In other words, copies the file from ``db_path`` to a temporary file
    and returns path of that temporary file. ``db_prefix`` is only used to generate
    the temporary file name. The temporary file is removed on exit.
    """
    fd, temp_path = mkstemp(prefix="impuls-merge", suffix=f"{db_prefix}.db")
    os.close(fd)
    try:
        shutil.copyfile(src=db_path, dst=temp_path)
        yield temp_path
    finally:
        os.remove(temp_path)


def pick_closest_stop(
    incoming: Stop,
    candidates: Iterable[Stop],
    max_distance_m: float,
) -> Stop | None:
    """Picks the closest stop from candidates to incoming, as long as the distance
    is not greater than the provided maximum.

    If there are no candidates within the provided ``max_distance_m`` radius, returns None.
    """
    closest, distance_m = min(
        ((s, earth_distance_m(incoming.lat, incoming.lon, s.lat, s.lon)) for s in candidates),
        default=(None, inf),
        key=itemgetter(1),
    )
    return closest if distance_m <= max_distance_m else None
