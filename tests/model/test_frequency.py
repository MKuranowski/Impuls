from typing import Type, final

from impuls.model import Frequency, TimePoint

from .template_entity import AbstractTestEntity


@final
class TestFrequency(AbstractTestEntity.Template[Frequency]):
    def get_entity(self) -> Frequency:
        return Frequency(
            trip_id="T0",
            start_time=TimePoint(hours=5),
            end_time=TimePoint(hours=8),
            headway=300,
            exact_times=True,
        )

    def get_type(self) -> Type[Frequency]:
        return Frequency

    def test_gtfs_marshall(self) -> None:
        self.assertDictEqual(
            self.get_entity().gtfs_marshall(),
            {
                "trip_id": "T0",
                "start_time": "05:00:00",
                "end_time": "08:00:00",
                "headway_secs": "300",
                "exact_times": "1",
            },
        )

    def test_gtfs_unmarshall(self) -> None:
        f = Frequency.gtfs_unmarshall(
            {
                "trip_id": "T0",
                "start_time": "05:00:00",
                "end_time": "08:00:00",
                "headway_secs": "300",
                "exact_times": "1",
            }
        )

        self.assertEqual(f.trip_id, "T0")
        self.assertEqual(f.start_time, TimePoint(hours=5))
        self.assertEqual(f.end_time, TimePoint(hours=8))
        self.assertEqual(f.headway, 300)
        self.assertTrue(f.exact_times)

    def test_sql_marshall(self) -> None:
        self.assertTupleEqual(
            self.get_entity().sql_marshall(),
            ("T0", 18000, 28800, 300, 1),
        )

    def test_sql_primary_key(self) -> None:
        self.assertTupleEqual(self.get_entity().sql_primary_key(), ("T0", 18000))

    def test_sql_unmarshall(self) -> None:
        f = Frequency.sql_unmarshall(("T0", 18000, 28800, 300, 1))

        self.assertEqual(f.trip_id, "T0")
        self.assertEqual(f.start_time, TimePoint(hours=5))
        self.assertEqual(f.end_time, TimePoint(hours=8))
        self.assertEqual(f.headway, 300)
        self.assertTrue(f.exact_times)