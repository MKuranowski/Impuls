from typing import Type

from impuls.model import StopTime, TimePoint

from .template_entity import AbstractTestEntity


class TestStopTime(AbstractTestEntity.Template[StopTime]):
    def get_entity(self) -> StopTime:
        return StopTime(
            trip_id="T0",
            stop_id="S0",
            stop_sequence=5,
            arrival_time=TimePoint(hours=10, minutes=10, seconds=0),
            departure_time=TimePoint(hours=10, minutes=10, seconds=30),
            pickup_type=StopTime.PassengerExchange.ON_REQUEST,
            drop_off_type=StopTime.PassengerExchange.ON_REQUEST,
            stop_headsign="",
            shape_dist_traveled=None,
            platform="A",
            extra_fields_json=None,
        )

    def get_type(self) -> Type[StopTime]:
        return StopTime

    def test_sql_marshall(self) -> None:
        self.assertTupleEqual(
            self.get_entity().sql_marshall(),
            ("T0", "S0", 5, 36600, 36630, 3, 3, "", None, "A", None),
        )

    def test_sql_marshall_past_midnight(self) -> None:
        st = self.get_entity()
        st.arrival_time = TimePoint(hours=25, minutes=10, seconds=0)
        st.departure_time = TimePoint(hours=25, minutes=10, seconds=30)

        self.assertTupleEqual(
            st.sql_marshall(),
            ("T0", "S0", 5, 90600, 90630, 3, 3, "", None, "A", None),
        )

    def test_sql_marshall_shape_dist_traveled(self) -> None:
        st = self.get_entity()
        st.shape_dist_traveled = 5.1

        self.assertTupleEqual(
            st.sql_marshall(),
            ("T0", "S0", 5, 36600, 36630, 3, 3, "", 5.1, "A", None),
        )

    def test_sql_primary_key(self) -> None:
        self.assertTupleEqual(self.get_entity().sql_primary_key(), ("T0", 5))

    def test_sql_unmarshall(self) -> None:
        st = StopTime.sql_unmarshall(("T0", "S0", 5, 36600, 36630, 3, 3, "", None, "A", None))

        self.assertEqual(st.trip_id, "T0")
        self.assertEqual(st.stop_id, "S0")
        self.assertEqual(st.stop_sequence, 5)
        self.assertEqual(st.arrival_time, TimePoint(hours=10, minutes=10, seconds=0))
        self.assertEqual(st.departure_time, TimePoint(hours=10, minutes=10, seconds=30))
        self.assertEqual(st.pickup_type, StopTime.PassengerExchange.ON_REQUEST)
        self.assertEqual(st.drop_off_type, StopTime.PassengerExchange.ON_REQUEST)
        self.assertEqual(st.stop_headsign, "")
        self.assertIsNone(st.shape_dist_traveled)
        self.assertEqual(st.platform, "A")
        self.assertIsNone(st.extra_fields_json)

    def test_sql_unmarshall_past_midnight(self) -> None:
        st = StopTime.sql_unmarshall(("T0", "S0", 5, 90600, 90630, 3, 3, "", None, "A", None))
        self.assertEqual(st.arrival_time, TimePoint(hours=25, minutes=10, seconds=0))
        self.assertEqual(st.departure_time, TimePoint(hours=25, minutes=10, seconds=30))

    def test_sql_unmarshall_shape_dist_traveled(self) -> None:
        st = StopTime.sql_unmarshall(("T0", "S0", 5, 36600, 36630, 3, 3, "", 5.1, "A", None))
        self.assertEqual(st.shape_dist_traveled, 5.1)
