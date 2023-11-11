from typing import cast

from impuls.model import (
    Agency,
    Calendar,
    CalendarException,
    Date,
    Route,
    Stop,
    StopTime,
    TimePoint,
    Trip,
)
from impuls.tasks import RemoveUnusedEntities

from .template_testcase import AbstractTestTask


class TestRemoveUnusedEntities(AbstractTestTask.Template):
    db_name = "wkd.db"

    def test(self) -> None:
        self.create_unused_entities()
        RemoveUnusedEntities().execute(self.runtime)
        self.assert_unused_entities_removed()

    def create_unused_entities(self) -> None:
        db = self.runtime.db
        db.create(Agency("X", "Unused", "https://example.com", "UTC"))
        db.create(Route("X", "X", "X", "Unused", Route.Type.RAIL))
        db.create(
            Stop(
                "X_station", "Unused", 52.12497, 20.74968, location_type=Stop.LocationType.STATION
            )
        )
        db.create(
            Stop(
                "X_stop",
                "Unused",
                52.12497,
                20.74968,
                location_type=Stop.LocationType.STOP,
                parent_station="X_station",
            )
        )
        db.create(
            Stop(
                "X_exit",
                "Unused",
                52.12497,
                20.74968,
                location_type=Stop.LocationType.EXIT,
                parent_station="X_station",
            )
        )
        db.create(
            Calendar(
                "X_no_trips",
                True,
                True,
                True,
                True,
                True,
                False,
                False,
                Date(2023, 4, 1),
                Date(2023, 4, 30),
            )
        )
        db.create(
            Calendar(
                "X_no_dates",
                True,
                True,
                True,
                True,
                True,
                True,
                True,
                Date(2023, 5, 1),
                Date(2023, 5, 3),
            )
        )
        db.create(
            CalendarException("X_no_dates", Date(2023, 5, 1), CalendarException.Type.REMOVED)
        )
        db.create(
            CalendarException("X_no_dates", Date(2023, 5, 2), CalendarException.Type.REMOVED)
        )
        db.create(
            CalendarException("X_no_dates", Date(2023, 5, 3), CalendarException.Type.REMOVED)
        )
        db.create(Trip("X_no_stop_times", "X", "X_no_trips"))
        db.create(Trip("X_one_stop_time", "X", "X_no_trips"))
        db.create(
            StopTime("X_one_stop_time", "X_stop", 0, TimePoint(hours=10), TimePoint(hours=10))
        )
        db.create(Trip("X_no_dates", "X", "X_no_dates"))
        db.create(StopTime("X_no_dates", "wsrod", 0, TimePoint(hours=10), TimePoint(hours=10)))
        db.create(
            StopTime(
                "X_no_dates",
                "wocho",
                1,
                TimePoint(hours=10, minutes=5),
                TimePoint(hours=10, minutes=5),
            )
        )

    def assert_unused_entities_removed(self) -> None:
        self.assert_unused_stop_times_removed()
        self.assert_unused_trips_removed()
        self.assert_unused_calendars_removed()
        self.assert_unused_stops_removed()
        self.assert_unused_routes_removed()
        self.assert_unused_agencies_removed()

    def assert_unused_stop_times_removed(self) -> None:
        trip_ids = {
            cast(str, i[0])
            for i in self.runtime.db.raw_execute("SELECT DISTINCT trip_id FROM stop_times")
        }
        self.assertNotIn("X_no_dates", trip_ids)
        self.assertNotIn("X_one_stop_time", trip_ids)
        self.assertEqual(self.runtime.db.count(StopTime), 6276)

    def assert_unused_trips_removed(self) -> None:
        trip_ids = {
            cast(str, i[0]) for i in self.runtime.db.raw_execute("SELECT trip_id FROM trips")
        }
        self.assertNotIn("X_no_dates", trip_ids)
        self.assertNotIn("X_one_stop_time", trip_ids)
        self.assertNotIn("X_no_stop_times", trip_ids)
        self.assertEqual(len(trip_ids), 372)

    def assert_unused_calendars_removed(self) -> None:
        self.assertSetEqual(
            {"C", "D"},
            {
                cast(str, i[0])
                for i in self.runtime.db.raw_execute("SELECT calendar_id FROM calendars")
            },
        )

    def assert_unused_stops_removed(self) -> None:
        stop_ids = {
            cast(str, i[0]) for i in self.runtime.db.raw_execute("SELECT stop_id FROM stops")
        }
        self.assertNotIn("X_station", stop_ids)
        self.assertNotIn("X_stop", stop_ids)
        self.assertNotIn("X_exit", stop_ids)
        self.assertEqual(len(stop_ids), 28)

    def assert_unused_routes_removed(self) -> None:
        self.assertSetEqual(
            {"A1", "ZA1", "ZA12"},
            {cast(str, i[0]) for i in self.runtime.db.raw_execute("SELECT route_id FROM routes")},
        )

    def assert_unused_agencies_removed(self) -> None:
        self.assertSetEqual(
            {"0"},
            {
                cast(str, i[0])
                for i in self.runtime.db.raw_execute("SELECT agency_id FROM agencies")
            },
        )
