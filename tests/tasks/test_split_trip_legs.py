from collections.abc import Iterable
from typing import Any, cast

from impuls import DBConnection
from impuls.model import Route, StopTime, TimePoint, Transfer, Trip
from impuls.tasks import SplitTripLegs

from .template_testcase import AbstractTestTask


class CustomSplitTripLegs(SplitTripLegs):
    def whole_trip_is_replacement_bus(self, trip: Trip) -> bool:
        return trip.id == "C-303"

    def get_departure_data(self, stop_time: StopTime) -> Any:
        return stop_time.trip_id == "C-105" and 13 <= stop_time.stop_sequence < 18

    def select_trip_ids(self, db: DBConnection) -> Iterable[str]:
        return ["C-303", "C-105", "C-5351"]


class TestSplitBusLegs(AbstractTestTask.Template):
    def test(self) -> None:
        t = CustomSplitTripLegs()
        t.execute(self.runtime)
        self.check_unchanged_trip()
        self.check_whole_bus_trip()
        self.check_multi_leg_trip()

    def check_unchanged_trip(self) -> None:
        t = self.runtime.db.retrieve_must(Trip, "C-5351")
        self.assertEqual(t.route_id, "ZA12")
        self.assertEqual(
            self.runtime.db.raw_execute(
                "SELECT COUNT(*) FROM stop_times WHERE trip_id = 'C-5351'",
            ).one_must("count must return one row")[0],
            3,
        )

    def check_whole_bus_trip(self) -> None:
        t = self.runtime.db.retrieve_must(Trip, "C-303")
        self.assertEqual(t.route_id, "A1_BUS")
        self.assertEqual(t.calendar_id, "C")
        self.assertEqual(t.short_name, "303")
        self.assertEqual(t.headsign, "Podkowa Leśna Główna")

        r = self.runtime.db.retrieve_must(Route, "A1_BUS")
        self.assertEqual(r.short_name, "A1")
        self.assertEqual(r.long_name, "Warszawa Śródmieście WKD — Grodzisk Mazowiecki Radońska")
        self.assertIs(r.type, Route.Type.BUS)

        st = list(
            self.runtime.db.typed_out_execute(
                "SELECT * FROM stop_times WHERE trip_id = 'C-303' ORDER BY stop_sequence ASC",
                StopTime,
            )
        )
        self.assertEqual(len(st), 19)

    def check_multi_leg_trip(self) -> None:
        self.assertSetEqual(
            {
                cast(str, i[0])
                for i in self.runtime.db.raw_execute(
                    "SELECT trip_id FROM trips WHERE trip_id LIKE 'C-105%'"
                )
            },
            {"C-105_0", "C-105_1", "C-105_2"},
        )
        self.check_multi_leg_trip_1()
        self.check_multi_leg_trip_2()
        self.check_multi_leg_trip_3()
        self.check_multi_leg_trip_transfers()

    def check_multi_leg_trip_1(self) -> None:
        t = self.runtime.db.retrieve_must(Trip, "C-105_0")
        self.assertEqual(t.route_id, "A1")
        self.assertEqual(t.short_name, "105")
        self.assertEqual(t.headsign, "Grodzisk Mazowiecki Radońska")

        st = list(
            self.runtime.db.typed_out_execute(
                "SELECT * FROM stop_times WHERE trip_id = 'C-105_0' ORDER BY stop_sequence ASC",
                StopTime,
            )
        )
        self.assertEqual(len(st), 14)
        self.assertEqual(st[0].stop_id, "wsrod")
        self.assertEqual(st[0].arrival_time, TimePoint(seconds=19200))
        self.assertEqual(st[0].departure_time, TimePoint(seconds=19200))
        self.assertNotEqual(st[0].platform, "BUS")
        self.assertEqual(st[-1].stop_id, "komor")
        self.assertEqual(st[-1].arrival_time, TimePoint(seconds=20940))
        self.assertEqual(st[-1].departure_time, TimePoint(seconds=20940))
        self.assertNotEqual(st[-1].platform, "BUS")

    def check_multi_leg_trip_2(self) -> None:
        t = self.runtime.db.retrieve_must(Trip, "C-105_1")
        self.assertEqual(t.route_id, "A1_BUS")
        self.assertEqual(t.short_name, "105")
        self.assertEqual(t.headsign, "Grodzisk Mazowiecki Radońska")

        st = list(
            self.runtime.db.typed_out_execute(
                "SELECT * FROM stop_times WHERE trip_id = 'C-105_1' ORDER BY stop_sequence ASC",
                StopTime,
            )
        )
        self.assertEqual(len(st), 6)
        self.assertEqual(st[0].stop_id, "komor")
        self.assertEqual(st[0].arrival_time, TimePoint(seconds=21000))
        self.assertEqual(st[0].departure_time, TimePoint(seconds=21000))
        self.assertEqual(st[0].platform, "BUS")
        self.assertEqual(st[-1].stop_id, "plglo")
        self.assertEqual(st[-1].arrival_time, TimePoint(seconds=21660))
        self.assertEqual(st[-1].departure_time, TimePoint(seconds=21660))
        self.assertEqual(st[-1].platform, "BUS")

    def check_multi_leg_trip_3(self) -> None:
        t = self.runtime.db.retrieve_must(Trip, "C-105_2")
        self.assertEqual(t.route_id, "A1")
        self.assertEqual(t.short_name, "105")
        self.assertEqual(t.headsign, "Grodzisk Mazowiecki Radońska")

        st = list(
            self.runtime.db.typed_out_execute(
                "SELECT * FROM stop_times WHERE trip_id = 'C-105_2' ORDER BY stop_sequence ASC",
                StopTime,
            )
        )
        self.assertEqual(len(st), 8)
        self.assertEqual(st[0].stop_id, "plglo")
        self.assertEqual(st[0].arrival_time, TimePoint(seconds=21840))
        self.assertEqual(st[0].departure_time, TimePoint(seconds=21840))
        self.assertNotEqual(st[0].platform, "BUS")
        self.assertEqual(st[-1].stop_id, "gmrad")
        self.assertEqual(st[-1].arrival_time, TimePoint(seconds=22980))
        self.assertEqual(st[-1].departure_time, TimePoint(seconds=22980))
        self.assertNotEqual(st[-1].platform, "BUS")

    def check_multi_leg_trip_transfers(self) -> None:
        t = list(
            self.runtime.db.typed_out_execute(
                "SELECT * FROM transfers WHERE from_trip_id LIKE 'C-105%' "
                "OR to_trip_id LIKE 'C-105%' "
                "ORDER BY from_trip_id ASC",
                Transfer,
            )
        )
        self.assertEqual(len(t), 2)

        self.assertEqual(t[0].from_stop_id, "komor")
        self.assertEqual(t[0].to_stop_id, "komor")
        self.assertEqual(t[0].from_trip_id, "C-105_0")
        self.assertEqual(t[0].to_trip_id, "C-105_1")
        self.assertIs(t[0].type, Transfer.Type.TIMED)

        self.assertEqual(t[1].from_stop_id, "plglo")
        self.assertEqual(t[1].to_stop_id, "plglo")
        self.assertEqual(t[1].from_trip_id, "C-105_1")
        self.assertEqual(t[1].to_trip_id, "C-105_2")
        self.assertIs(t[1].type, Transfer.Type.TIMED)
