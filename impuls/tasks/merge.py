import os
import shutil
from contextlib import contextmanager
from dataclasses import dataclass
from itertools import count
from math import inf
from operator import itemgetter
from tempfile import mkstemp
from typing import Container, Generator, Iterable, NamedTuple, Type, cast, final

from ..db import DBConnection
from ..model import FeedInfo, Route, Stop
from ..task import Task, TaskRuntime
from ..tools.geo import earth_distance_m
from ..tools.types import Self, all_non_none


@dataclass(frozen=True)
class DatabaseToMerge:
    resource_name: str
    prefix: str


@dataclass(frozen=True)
class RouteHash:
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
    ids_to_change: list[tuple[str, str]]
    """(new_id, old_id) pairs of ids that need to be changes"""

    total: int
    merged: int


@final
class Merge(Task):
    """Merge tasks inserts data from provided impuls databases into the current one.

    The user must ensure that `f"{db_to_merge.prefix}{separator}{id}" generates unique
    ids across all data. This especially applies if the runtime database already has data.

    This tasks also performs merging of some entity types, provided they have the same ID:

    - Agency and Attribution instances are always merged - attributes of the first encountered
        instance are kept.
    - Calendar, CalendarException, Trip and StopTime instances are never merged -
        incoming ids are always prefixed by the database_to_merge prefix.
    - Route instances are merged if they have the same agency_id, short_name, type and color.
        Other attributes of the first encountered instance are kept.
        If the comparison attributes do not match, the incoming id will have a numeric suffix
        added.
    - Stop instances are merged if they have the same name, code, zone_id, location_type,
        parent_station, wheelchair_boarding, platform_code and are within 10m of each other.
        Other attributes of the first encountered instance are kept.
        If the comparison attributes do not match, the incoming id will have a numeric suffix
        added.
    - FeedInfo is treated specially:
        - If it exists in the current (runtime) database, it is kept as-is,
            and any other instances are ignored.
        - If **all** to-merge databases have a FeedInfo object, then the attributes of the first
            encountered one are kept, except that:
                - feed_start_date is set to the min of all encountered feed_start_date
                - feed_end_date is set to the max of all encountered feed_end_date
                - feed_version is set to all encountered feed_versions, separated with
                    `feed_version_separator`
        - Otherwise, no FeedInfo object will be created.
        The first case is meant for merging on smaller, helper datasets to an already-loaded
        major database.
        The second case serves merging versioned datasets, with the last case preventing
        any inconsistencies in the FeedInfo object.
    - Stop.zone_id is left untouched - effectively merging zones across datasets.
    - Trip.block_id and Trip.shape_id are prefixed with db_to_merge.prefix - effectively
        never merging blocks or shapes across datasets.
    """

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
        self.known_routes: dict[RouteHash, str] = {}
        self.used_route_ids: set[str] = set()
        self.known_stops: dict[StopHash, list[Stop]] = {}
        self.used_stop_ids: set[str] = set()

        self.feed_infos: list[FeedInfo | None] | None = None
        """None if FeedInfo should not be collected, otherwise list of FeedInfos from merged dbs"""

    def execute(self, r: TaskRuntime) -> None:
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
            self.merge(r.db, db_to_merge_path, db_to_merge.prefix)

        self.logger.info("Resolving FeedInfo")
        self.insert_feed_info(r.db)

    def initialize_known_objects(self, db: DBConnection) -> None:
        self.initialize_known_routes(db)
        self.initialize_known_stops(db)
        self.initialize_known_feed_info(db)

    def initialize_known_routes(self, db: DBConnection) -> None:
        self.known_routes.clear()
        self.used_route_ids.clear()
        for route in db.retrieve_all(Route):
            self.used_route_ids.add(route.id)
            self.known_routes[RouteHash.of(route)] = route.id

    def initialize_known_stops(self, db: DBConnection) -> None:
        self.known_stops.clear()
        self.used_stop_ids.clear()
        for stop in db.retrieve_all(Stop):
            self.used_stop_ids.add(stop.id)
            self.known_stops[StopHash.of(stop)] = [stop]

    def initialize_known_feed_info(self, db: DBConnection) -> None:
        feed_info_count = cast(
            int,
            db.raw_execute("SELECT COUNT(*) FROM feed_info").one_must("COUNT must have rows")[0],
        )
        self.feed_infos = None if feed_info_count > 0 else []

    def merge(self, db: DBConnection, incoming_path: str, incoming_prefix: str) -> None:
        # To copy objects from the incoming db, its prefix needs to be to the id columns.
        # This means that the incoming db will be mutated. To prevent multiple mutations
        # if the db is re-used across runs, it is copied into a temporary file.
        with (
            temp_db_file(incoming_path, incoming_prefix) as incoming_mut_path,
            attached(db, incoming_mut_path),
            db.transaction(),
        ):
            self.merge_with_attached(db, incoming_prefix)

    def merge_with_attached(self, db: DBConnection, incoming_prefix: str) -> None:
        self.merge_agencies(db)
        self.merge_attributions(db)
        self.merge_routes(db)
        self.merge_stops(db)
        self.merge_calendars(db, incoming_prefix)
        self.merge_calendar_exceptions(db)
        self.merge_trips(db, incoming_prefix)
        self.merge_stop_times(db)
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
        # At this point, only to-be-merged routes have the same ids - it's safe to ignore conflicts
        db.raw_execute("INSERT OR IGNORE INTO routes SELECT * FROM incoming.routes")

    def resolve_route_conflicts(self, db: DBConnection) -> ConflictResolution:
        incoming_ids_to_change: list[tuple[str, str]] = []
        total = 0
        merged = 0

        for incoming_route in db.typed_out_execute("SELECT * FROM incoming.routes", Route):
            total += 1
            hash = RouteHash.of(incoming_route)
            new_id = self.known_routes.get(hash)

            if new_id is None:
                new_id = find_non_conflicting_id(
                    self.used_route_ids,
                    incoming_route.id,
                    self.separator,
                )
                self.used_route_ids.add(new_id)
                self.known_routes[hash] = new_id
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
                self.known_stops.get(hash, []),
                self.distance_between_similar_stops_m,
            )

            if similar_stop is not None:
                merged += 1
                new_id = similar_stop.id

            else:
                new_id = find_non_conflicting_id(
                    self.used_stop_ids,
                    incoming_stop.id,
                    self.separator,
                )
                self.used_stop_ids.add(new_id)
                self.known_stops.setdefault(hash, []).append(incoming_stop)

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

    def merge_trips(self, db: DBConnection, incoming_prefix: str) -> None:
        self.logger.debug("Joining Trips")

        # NOTE: merge_routes should have updated the route_id
        # NOTE: merge_calendars should have updated the calendar_id
        db.raw_execute(
            "UPDATE incoming.trips SET trip_id = ? || ? || trip_id",
            (incoming_prefix, self.separator),
        )
        db.raw_execute(
            "UPDATE incoming.trips SET block_id = ? || ? || block_id WHERE block_id IS NOT NULL",
            (incoming_prefix, self.separator),
        )
        db.raw_execute(
            "UPDATE incoming.trips SET shape_id = ? || ? || shape_id WHERE shape_id IS NOT NULL",
            (incoming_prefix, self.separator),
        )
        db.raw_execute("INSERT OR ABORT INTO trips SELECT * FROM incoming.trips")

    def merge_stop_times(self, db: DBConnection) -> None:
        self.logger.debug("Joining StopTimes")

        # NOTE: merge_stops should have updated the stop_id
        # NOTE: merge_trips should have updated the trip_id
        db.raw_execute("INSERT OR ABORT INTO stop_times SELECT * FROM incoming.stop_times")

    def collect_incoming_feed_info(self, db: DBConnection) -> None:
        self.logger.debug("Collecting FeedInfo")

        if self.feed_infos is not None:
            feed_info = db.typed_out_execute("SELECT * FROM incoming.feed_info", FeedInfo).one()
            self.feed_infos.append(feed_info)

    def insert_feed_info(self, db: DBConnection) -> None:
        # Shallow copy - pyright can't narrow the type to list[FeedInfo] after a type guard
        # when operating directly on the class attribute.
        feed_infos = self.feed_infos

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
    """Attaches a database from the provided path as `incoming`.
    Detached that database on exit.
    """
    db.raw_execute("ATTACH DATABASE ? as incoming", (path_to_incoming,))
    try:
        yield
    finally:
        db.raw_execute("DETACH DATABASE incoming")


@contextmanager
def temp_db_file(db_path: str, db_prefix: str) -> Generator[str, None, None]:
    """Creates a temporary copy of a database, so that it can be mutated
    without the changes being visible in db_path.

    In other words, copies the file from `db_path` to a temporary file
    and returns path of that temporary file. `db_prefix` is only used to generate
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

    If there are no candidates at all, or there are no candidates max_distance_m radius,
    returns None.
    """
    closest, distance_m = min(
        ((s, earth_distance_m(incoming.lat, incoming.lon, s.lat, s.lon)) for s in candidates),
        default=(None, inf),
        key=itemgetter(1),
    )
    return closest if distance_m <= max_distance_m else None


def find_non_conflicting_id(used: Container[str], id: str, separator: str = ":") -> str:
    """Tries to find the lowest numeric suffix (joined with separator) to the id
    which generates a string not contained in `used`.

    >>> find_non_conflicting_id({"A", "B"}, "C")
    'C'
    >>> find_non_conflicting_id({"A", "B"}, "A")
    'A:1'
    >>> find_non_conflicting_id({"A", "A/1", "A/2"}, "A", separator="/")
    'A/3'
    """
    if id not in used:
        return id

    for suffix in count(1):
        candidate = f"{id}{separator}{suffix}"
        if candidate not in used:
            return candidate

    raise RuntimeError("not reachable - itertools.count must be infinite")
