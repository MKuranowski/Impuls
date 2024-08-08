# © Copyright 2022-2024 Mikołaj Kuranowski
# SPDX-License-Identifier: GPL-3.0-or-later

from ..db import DBConnection
from ..model import Calendar, CalendarException
from ..task import Task, TaskRuntime


class RemoveUnusedEntities(Task):
    """RemoveUnusedEntities removes entities from the database which serve no purpose:

    * :py:class:`Trips <impuls.model.Trip>` with 0 or 1 :py:class:`~impuls.model.StopTime`,
    * :py:class:`Calendars <impuls.model.Calendar>` with no :py:class:`Trips <impuls.model.Trip>`,
    * :py:class:`Calendars <impuls.model.Calendar>` without any active dates,
    * :py:class:`Stops <impuls.model.Stop>` (with
      :py:obj:`LocationType.STOP <impuls.model.Stop.LocationType.STOP>`) with no
      :py:class:`StopTimes <impuls.model.StopTime>`,
    * :py:class:`Stations <impuls.model.Stop>` (with
      :py:obj:`LocationType.STATION <impuls.model.Stop.LocationType.STATION>`) with no child
      :py:class:`Stops <impuls.model.Stop>`,
    * :py:class:`Routes <impuls.model.Route>` with no :py:class:`Trips <impuls.model.Trip`,
    * :py:class:`Agencies <impuls.model.Agency>` with no :py:class:`Routes <impuls.model.Route>`.
    """

    def __init__(self) -> None:
        super().__init__()

    def execute(self, r: TaskRuntime) -> None:
        self.drop_trips_with_at_most_one_stop(r.db)
        self.drop_calendars_without_trips(r.db)
        self.drop_calendars_without_dates(r.db)
        self.drop_stops_without_stop_times(r.db)
        self.drop_stations_without_stops(r.db)
        self.drop_routes_without_trips(r.db)
        self.drop_agencies_without_routes(r.db)

    def drop_trips_with_at_most_one_stop(self, db: DBConnection) -> None:
        result = db.raw_execute(
            "DELETE FROM trips WHERE "
            "(SELECT COUNT(*) FROM stop_times WHERE stop_times.trip_id = trips.trip_id) <= 1"
        )
        self.logger.info("Dropped %d trip(s) with 0 or 1 stop_time", result.rowcount)

    def drop_calendars_without_trips(self, db: DBConnection) -> None:
        result = db.raw_execute(
            "DELETE FROM calendars WHERE NOT EXISTS "
            "(SELECT trip_id FROM trips WHERE trips.calendar_id = calendars.calendar_id)"
        )
        self.logger.info("Dropped %d calendar(s) without any trips", result.rowcount)

    def drop_calendars_without_dates(self, db: DBConnection) -> None:
        calendars_without_dates = list[tuple[str]]()

        for calendar in db.retrieve_all(Calendar):
            active_dates = CalendarException.reflect_in_active_dates(
                calendar.compute_active_dates(),
                db.typed_out_execute(
                    "SELECT * FROM :table WHERE calendar_id = ?",
                    CalendarException,
                    (calendar.id,),
                ),
            )
            if not active_dates:
                calendars_without_dates.append((calendar.id,))

        result = db.raw_execute_many(
            "DELETE FROM calendars WHERE calendar_id = ?",
            calendars_without_dates,
        )
        self.logger.info("Dropped %d calendar(s) without any dates", result.rowcount)

    def drop_stops_without_stop_times(self, db: DBConnection) -> None:
        result = db.raw_execute(
            "DELETE FROM stops WHERE location_type = 0 AND "
            "NOT EXISTS (SELECT stop_id FROM stop_times WHERE stop_times.stop_id = stops.stop_id)"
        )
        self.logger.info("Dropped %d stop(s) without any stop times", result.rowcount)

    def drop_stations_without_stops(self, db: DBConnection) -> None:
        result = db.raw_execute(
            "DELETE FROM stops AS s1 WHERE s1.location_type = 1 AND NOT EXISTS "
            "(SELECT stop_id FROM stops s2 WHERE "
            " s2.location_type = 0 AND s2.parent_station = s1.stop_id)"
        )
        self.logger.info("Dropped %d station(s) without any platforms", result.rowcount)

    def drop_routes_without_trips(self, db: DBConnection) -> None:
        result = db.raw_execute(
            "DELETE FROM routes WHERE NOT EXISTS "
            "(SELECT trip_id FROM trips WHERE trips.route_id = routes.route_id)"
        )
        self.logger.info("Dropped %d calendar(s) without any trips", result.rowcount)

    def drop_agencies_without_routes(self, db: DBConnection) -> None:
        result = db.raw_execute(
            "DELETE FROM agencies WHERE NOT EXISTS "
            "(SELECT route_id FROM routes WHERE routes.agency_id = agencies.agency_id)"
        )
        self.logger.info("Dropped %d calendar(s) without any trips", result.rowcount)
