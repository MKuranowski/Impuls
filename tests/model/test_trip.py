from typing import Type

from impuls.model import Trip

from .template_entity import AbstractTestEntity


class TestAgency(AbstractTestEntity.Template[Trip]):
    def get_entity(self) -> Trip:
        return Trip(
            id="0",
            route_id="R0",
            calendar_id="C0",
            headsign="Foo",
            short_name="",
            direction=Trip.Direction.OUTBOUND,
            block_id="B0",
            shape_id="S0",
            wheelchair_accessible=True,
            bikes_allowed=False,
            exceptional=False,
            extra_fields_json=r'{"hidden_block_id":"B0"}',
        )

    def get_type(self) -> Type[Trip]:
        return Trip

    def test_sql_marshall(self) -> None:
        self.assertTupleEqual(
            self.get_entity().sql_marshall(),
            ("0", "R0", "C0", "Foo", "", 0, "B0", "S0", 1, 0, 0, r'{"hidden_block_id":"B0"}'),
        )

    def test_sql_marshall_unknowns(self) -> None:
        t = self.get_entity()
        t.direction = None
        t.block_id = ""
        t.shape_id = ""
        t.wheelchair_accessible = None
        t.bikes_allowed = None
        t.exceptional = None
        t.extra_fields_json = None

        self.assertTupleEqual(
            t.sql_marshall(),
            ("0", "R0", "C0", "Foo", "", None, None, None, None, None, None, None),
        )

    def test_sql_primary_key(self) -> None:
        self.assertTupleEqual(self.get_entity().sql_primary_key(), ("0",))

    def test_sql_unmarshall(self) -> None:
        t = Trip.sql_unmarshall(
            ("0", "R0", "C0", "Foo", "", 0, "B0", "S0", 1, 0, 0, r'{"hidden_block_id":"B0"}'),
        )

        self.assertEqual(t.id, "0")
        self.assertEqual(t.route_id, "R0")
        self.assertEqual(t.calendar_id, "C0")
        self.assertEqual(t.headsign, "Foo")
        self.assertEqual(t.short_name, "")
        self.assertEqual(t.direction, Trip.Direction.OUTBOUND)
        self.assertEqual(t.block_id, "B0")
        self.assertEqual(t.shape_id, "S0")
        self.assertEqual(t.wheelchair_accessible, True)
        self.assertEqual(t.bikes_allowed, False)
        self.assertEqual(t.exceptional, False)
        self.assertEqual(t.extra_fields_json, r'{"hidden_block_id":"B0"}')
        self.assertDictEqual(t.get_extra_fields(), {"hidden_block_id": "B0"})

    def test_sql_unmarshall_unknowns(self) -> None:
        t = Trip.sql_unmarshall(
            ("0", "R0", "C0", "Foo", "", None, None, None, None, None, None, None)
        )

        self.assertIsNone(t.direction)
        self.assertEqual(t.block_id, "")
        self.assertEqual(t.shape_id, "")
        self.assertIsNone(t.wheelchair_accessible)
        self.assertIsNone(t.bikes_allowed)
        self.assertIsNone(t.exceptional)
        self.assertIsNone(t.extra_fields_json)
