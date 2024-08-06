from typing import Type

from impuls.model import Route

from .template_entity import AbstractTestEntity


class TestRoute(AbstractTestEntity.Template[Route]):
    def get_entity(self) -> Route:
        return Route(
            id="A",
            agency_id="0",
            short_name="A",
            long_name="Foo - Bar",
            type=Route.Type.BUS,
            color="BB0000",
            text_color="FFFFFF",
            sort_order=None,
        )

    def get_type(self) -> Type[Route]:
        return Route

    def test_sql_marshall(self) -> None:
        self.assertTupleEqual(
            self.get_entity().sql_marshall(),
            ("A", "0", "A", "Foo - Bar", 3, "BB0000", "FFFFFF", None),
        )

    def test_sql_primary_key(self) -> None:
        self.assertTupleEqual(self.get_entity().sql_primary_key(), ("A",))

    def test_sql_unmarshall(self) -> None:
        r = Route.sql_unmarshall(("A", "0", "A", "Foo - Bar", 3, "BB0000", "FFFFFF", None))

        self.assertEqual(r.id, "A")
        self.assertEqual(r.agency_id, "0")
        self.assertEqual(r.short_name, "A")
        self.assertEqual(r.long_name, "Foo - Bar")
        self.assertEqual(r.type, Route.Type.BUS)
        self.assertEqual(r.color, "BB0000")
        self.assertEqual(r.text_color, "FFFFFF")
        self.assertEqual(r.sort_order, None)
