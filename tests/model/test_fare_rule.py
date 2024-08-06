from typing import Type

from impuls.model import FareRule

from .template_entity import AbstractTestEntity


class TestFareRule(AbstractTestEntity.Template[FareRule]):
    def get_entity(self) -> FareRule:
        return FareRule(
            fare_id="F0",
            route_id="R0",
            contains_id="Z3",
            id=1,
        )

    def get_type(self) -> Type[FareRule]:
        return FareRule

    def test_sql_marshall(self) -> None:
        self.assertTupleEqual(
            self.get_entity().sql_marshall(),
            ("F0", "R0", None, None, "Z3"),
        )

    def test_sql_primary_key(self) -> None:
        self.assertTupleEqual(self.get_entity().sql_primary_key(), (1,))

    def test_sql_unmarshall(self) -> None:
        t = FareRule.sql_unmarshall((1, "F0", "R0", None, None, "Z3"))

        self.assertEqual(t.id, 1)
        self.assertEqual(t.fare_id, "F0")
        self.assertEqual(t.route_id, "R0")
        self.assertEqual(t.origin_id, "")
        self.assertEqual(t.destination_id, "")
        self.assertEqual(t.contains_id, "Z3")
