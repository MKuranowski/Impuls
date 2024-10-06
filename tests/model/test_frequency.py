from typing import Type

from impuls.model import Frequency, TimePoint

from .template_entity import AbstractTestEntity


class TestFrequency(AbstractTestEntity.Template[Frequency]):
    def get_entity(self) -> Frequency:
        return Frequency(
            trip_id="T0",
            start_time=TimePoint(hours=5),
            end_time=TimePoint(hours=8),
            headway=300,
            exact_times=True,
            extra_fields_json=None,
        )

    def get_type(self) -> Type[Frequency]:
        return Frequency

    def test_sql_marshall(self) -> None:
        self.assertTupleEqual(
            self.get_entity().sql_marshall(),
            ("T0", 18000, 28800, 300, 1, None),
        )

    def test_sql_primary_key(self) -> None:
        self.assertTupleEqual(self.get_entity().sql_primary_key(), ("T0", 18000))

    def test_sql_unmarshall(self) -> None:
        f = Frequency.sql_unmarshall(("T0", 18000, 28800, 300, 1, None))

        self.assertEqual(f.trip_id, "T0")
        self.assertEqual(f.start_time, TimePoint(hours=5))
        self.assertEqual(f.end_time, TimePoint(hours=8))
        self.assertEqual(f.headway, 300)
        self.assertTrue(f.exact_times)
        self.assertIsNone(f.extra_fields_json)
