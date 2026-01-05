# © Copyright 2024 Mikołaj Kuranowski
# SPDX-License-Identifier: GPL-3.0-or-later

import re
from collections.abc import Iterable
from copy import copy
from typing import Any, TypeAlias, cast

from .. import selector
from ..db import DBConnection
from ..model import Route, StopTime, Transfer, Trip
from ..task import Task, TaskRuntime


class SplitTripLegs(Task):
    """SplitTripLegs splits :py:class:`Trips <impuls.model.Trip>` into multiple legs
    with different attributes, generating new :py:class:`Routes <impuls.model.Route>`
    and :py:class:`Transfers <impuls.model.Transfer>` on the go.

    This task can be customized by subclassing and overriding specific methods.

    The default configuration is meant for separating out bus replacement services
    for trains. Bus replacement service **departures** are identified by
    :py:attr:`StopTime.platform <impuls.model.StopTime.platform>` set to ``BUS``.
    Bus legs get assigned to a copy of the original :py:class:`~impuls.model.Route` with the
    type updated and ID suffixed by ``_BUS``. :py:obj:`~impuls.model.Transfer.Type.TIMED`
    transfers are also generated. In this configuration ``data`` is a boolean, flag
    set on bus departures.
    """

    Leg: TypeAlias = tuple[list[StopTime], Any]

    route_selector: selector.Routes
    """Selects which routes' trips should be separated by this step."""

    replacement_bus_short_name_pattern: re.Pattern[str] | None
    """:py:attr:`Trip.short_name <impuls.model.Trip.short_name>` pattern indicating
    that the whole trip is operated by a bus replacement service.
    """

    leg_trip_id_infix: str

    added_routes: set[str]

    def __init__(
        self,
        route_selector: selector.Routes = selector.Routes(type=Route.Type.RAIL),
        replacement_bus_short_name_pattern: re.Pattern[str] | None = None,
        leg_trip_id_infix: str = "_",
    ) -> None:
        super().__init__()
        self.route_selector = route_selector
        self.replacement_bus_short_name_pattern = replacement_bus_short_name_pattern
        self.leg_trip_id_infix = leg_trip_id_infix
        self.added_routes = set()

    def execute(self, r: TaskRuntime) -> None:
        self.added_routes.clear()
        to_process = list(self.select_trip_ids(r.db))
        with r.db.transaction():
            for i, trip_id in enumerate(to_process, start=1):
                self.process_trip(trip_id, r.db)
                if i % 500 == 0:
                    self.logger.info("Processed %d/%d trips", i, len(to_process))

    def select_trip_ids(self, db: DBConnection) -> Iterable[str]:
        """Selects which trips should be processed by this step.
        Defaults to all trips belonging to routes selected by :py:attr:`.route_selector`.
        """

        for route_id in self.route_selector.find_ids(db):
            yield from (
                cast(str, i[0])
                for i in db.raw_execute(
                    "SELECT trip_id FROM trips WHERE route_id = ?",
                    (route_id,),
                )
            )

    def process_trip(self, trip_id: str, db: DBConnection) -> None:
        """Called by :py:meth:`.execute` on every selected trip.
        Default is to retrieve the objects from the database and then call :py:meth:`compute_legs`
        and either :py:meth:`update_trip_with_single_leg` or :py:meth:`replace_trip_by_legs`,
        depending if there is one or more legs.
        """
        original_trip = db.retrieve_must(Trip, trip_id)
        stop_times = list(
            db.typed_out_execute("SELECT * FROM stop_times WHERE trip_id=?", StopTime, (trip_id,))
        )

        legs = self.compute_legs(original_trip, stop_times)
        if len(legs) == 1:
            self.update_trip_with_single_leg(original_trip, legs[0][1], db)
        else:
            self.replace_trip_by_legs(original_trip, legs, db)

    def compute_legs(self, original_trip: Trip, stop_times: list[StopTime]) -> list[Leg]:
        """Splits the provided list of :py:class:`StopTimes <impuls.model.StopTime>`
        into multiple legs.

        The default algorithm keeps track of the return value of :py:meth:`.get_departure_data`,
        and creates new legs when that value changes. This first :py:class:`~impuls.model.StopTime`
        with new ``data`` is assumed to be belonging to both legs - the result of
        :py:meth:`.arrival_only` is appended to the previous leg, while the result of
        :py:meth:`.departure_only` is appended to the current leg. As en example,
        the following stop_times:

        * ``StopTime(0, data=False)``
        * ``StopTime(1, data=False)``
        * ``StopTime(2, data=True)``
        * ``StopTime(3, data=True)``
        * ``StopTime(4, data=False)``
        * ``StopTime(5, data=False)``

        Are separated into the following legs:

        * Leg 0, ``data=False``:

          * ``StopTime(0)``
          * ``StopTime(1)``
          * ``arrival_only(StopTime(2), data=False)``

        * Leg 1, ``data=True``:

          * ``departure_only(StopTime(2), data=True)``
          * ``StopTime(3)``
          * ``arrival_only(StopTime(4), data=True)``

        * Leg 2, ``data=False``:

          * ``departure_only(StopTime(4), data=False)``
          * ``StopTime(5)``

        As a special case, if :py:meth:`.whole_trip_is_replacement_bus` returns true,
        this function short-circuits to returning ``[(stop_times, True)]``.
        """
        if self.whole_trip_is_replacement_bus(original_trip):
            return [(stop_times, True)]

        legs = list[self.Leg]()
        leg = list[StopTime]()
        previous_data = self.get_departure_data(stop_times[0])

        for stop_time in stop_times:
            current_data = self.get_departure_data(stop_time)

            if previous_data != current_data:
                if leg:
                    leg.append(self.arrival_only(stop_time, previous_data))
                    legs.append((leg, previous_data))

                leg = [self.departure_only(stop_time, current_data)]
                previous_data = current_data
            else:
                leg.append(stop_time)

        if len(leg) > 1:
            legs.append((leg, previous_data))

        return legs

    def update_trip_with_single_leg(self, trip: Trip, data: Any, db: DBConnection) -> None:
        """Called by :py:meth:`.process_trip` for trips with a single leg.
        The default implementation simply calls :py:meth:`.update_trip` followed by
        :py:meth:`db.update <impuls.DBConnection.update>` if ``data`` is truthy.
        """
        if data:
            self.update_trip(trip, data, db)
            db.update(trip)

    def replace_trip_by_legs(self, original_trip: Trip, legs: list[Leg], db: DBConnection) -> None:
        """Replaces an existing :py:class:`~impuls.model.Trip` by multiple instances,
        as represented by ``legs``. Called by :py:meth:`.process_trip` for trips with
        multiple legs.

        The default implementation removes the ``original_trip``, and then for every leg:

        * creates a new trip for each leg, as modified by :py:meth:`.update_trip`, with the ID
          suffixed by `_0`, `_1`, `_2`, ..., `leg_trip_id_infix` can be changed to customize
          the generated suffix;
        * re-inserts :py:class:`StopTimes <impuls.model.StopTime>` with only their
          :py:attr:`~impuls.model.StopTime.trip_id` changed;
        * creates :py:class:`Transfers <impuls.model.Transfer>` between every leg,
          as returned by :py:meth:`.create_transfer`.
        """

        # Remove the original trip
        db.raw_execute("DELETE FROM trips WHERE trip_id = ?", (original_trip.id,))

        previous_trip: Trip | None = None

        for idx, (stop_times, data) in enumerate(legs):
            # Create a trip for the current leg
            trip = copy(original_trip)
            trip.id = f"{trip.id}{self.leg_trip_id_infix}{idx}"
            self.update_trip(trip, data, db)
            db.create(trip)

            # Insert stop_times of this leg
            for stop_time in stop_times:
                stop_time.trip_id = trip.id
                db.create(stop_time)

            # Insert a transfer between this and the previous leg
            if previous_trip is not None:
                t = self.get_transfer(previous_trip, trip, stop_times[0].stop_id)
                if t is not None:
                    db.create(t)

            previous_trip = trip

    def update_trip(self, trip: Trip, data: Any, db: DBConnection) -> None:
        """Modifies the attributes of a :py:class:`~impuls.model.Trip` representing
        a single leg. Called by :py:meth:`.update_trip_with_single_leg` and
        :py:meth:`.replace_trip_by_legs`.

        The default behavior depends on the value of ``data``.
        If it is truthy, the trip's :py:attr:`~impuls.model.Trip.route_id` is suffixed by
        ``_BUS``, and a new route is created by calling
        :py:meth:`.save_bus_replacement_route_in_db` (if it was not created before, as
        indicated by the :py:attr:`.added_routes` set).
        Otherwise, the trip is left as-is.
        """

        if data:
            new_route_id = f"{trip.route_id}_BUS"
            if new_route_id not in self.added_routes:
                self.save_bus_replacement_route_in_db(trip.route_id, new_route_id, db)
                self.added_routes.add(new_route_id)
            trip.route_id = new_route_id

    def save_bus_replacement_route_in_db(
        self,
        original_route_id: str,
        new_route_id: str,
        db: DBConnection,
    ) -> None:
        """Saves a bus replacement route in the :py:class:`database <impuls.DBConnection>`.
        The default behavior is to create a copy of the original :py:class:`~impuls.model.Route`,
        call :py:meth:`.update_bus_replacement_route`, followed by
        :py:meth:`db.create <impuls.DBConnection.create>`.
        """
        route = db.retrieve_must(Route, original_route_id)
        route.id = new_route_id
        self.update_bus_replacement_route(route)
        db.create(route)

    def update_bus_replacement_route(self, route: Route) -> None:
        """Updates the attributes of a bus-replacement route. Defaults to setting
        the :py:attr:`~impuls.model.Route.type` to :py:obj:`~impuls.model.Route.Type.BUS`.
        """
        route.type = Route.Type.BUS

    def get_departure_data(self, stop_time: StopTime) -> Any:
        """Extracts leg-identifying data of the departure represented by the provided
        :py:class:`~impuls.model.StopTime`. The default behavior is to flag bus replacement
        service by returning ``stop_time.platform == "BUS"``.
        """
        return stop_time.platform == "BUS"

    def arrival_only(self, stop_time: StopTime, previous_data: Any) -> StopTime:
        """Creates a copy of a :py:class:`~impuls.model.StopTime` for an arrival-only,
        last stop of a trip. The second argument is the return value of
        :py:meth:`.get_departure_data` of the **preceding** :py:class:`~impuls.model.StopTime`.
        See :py:meth:`.compute_legs` for details.

        The default behavior is to `copy <https://docs.python.org/3/library/copy.html#copy.copy>`_
        the stop_time, set its :py:attr:`~impuls.model.StopTime.departure_time` to be the same
        as the :py:attr:`~impuls.model.StopTime.arrival_time`, and ensure the
        :py:attr:`~impuls.model.StopTime.platform` is set to ``"BUS"`` if and only if
        ``previous_data`` is truthy.
        """
        new = copy(stop_time)
        new.departure_time = new.arrival_time
        if previous_data:
            new.platform = "BUS"
        elif new.platform == "BUS":
            new.platform = ""
        return new

    def departure_only(self, stop_time: StopTime, current_data: Any) -> StopTime:
        """Creates a copy of a :py:class:`~impuls.model.StopTime` for a departure-only,
        first stop of a trip. The second argument is the return value of
        :py:meth:`.get_departure_data` of the this :py:class:`~impuls.model.StopTime`.
        See :py:meth:`.compute_legs` for details.

        The default behavior is to `copy <https://docs.python.org/3/library/copy.html#copy.copy>`_
        the stop_time, set its :py:attr:`~impuls.model.StopTime.arrival_time` to be the same
        as the :py:attr:`~impuls.model.StopTime.departure_time`, and ensure the
        :py:attr:`~impuls.model.StopTime.platform` is set to ``"BUS"`` if and only if
        ``current_data`` is truthy.
        """
        new = copy(stop_time)
        new.arrival_time = new.departure_time
        if current_data:
            new.platform = "BUS"
        elif new.platform == "BUS":
            new.platform = ""
        return new

    def whole_trip_is_replacement_bus(self, trip: Trip) -> bool:
        """Returns True if the whole :py:class:`~impuls.model.Trip` is operated
        by a replacement bus service. Defaults to
        `searching <https://docs.python.org/3/library/re.html#re.Pattern.search>`_ the
        :py:attr:`.replacement_bus_short_name_pattern` in the
        :py:attr:`Trip.short_name <impuls.model.Trip.short_name>`. If
        :py:attr:`.replacement_bus_short_name_pattern` is ``None``, returns ``False``.
        """
        return (
            self.replacement_bus_short_name_pattern is not None
            and self.replacement_bus_short_name_pattern.search(trip.short_name) is not None
        )

    def get_transfer(self, trip_a: Trip, trip_b: Trip, transfer_stop_id: str) -> Transfer | None:
        """Creates a :py:class:`~impuls.model.Transfer` object linking to legs of a trip.
        Defaults to creating a :py:obj:`~impuls.model.Transfer.Type.TIMED` transfer.
        """
        return Transfer(
            from_stop_id=transfer_stop_id,
            to_stop_id=transfer_stop_id,
            from_trip_id=trip_a.id,
            to_trip_id=trip_b.id,
            type=Transfer.Type.TIMED,
        )
