from typing import Type, final

from impuls.model import Transfer

from .template_entity import AbstractTestEntity


@final
class TestTransfer(AbstractTestEntity.Template[Transfer]):
    def get_entity(self) -> Transfer:
        return Transfer(
            from_stop_id="S0",
            to_stop_id="S1",
            from_route_id="",
            to_route_id="",
            from_trip_id="T0",
            to_trip_id="T1",
            type=Transfer.Type.TIMED,
            id=1,
        )

    def get_type(self) -> Type[Transfer]:
        return Transfer

    def test_sql_marshall(self) -> None:
        self.assertTupleEqual(
            self.get_entity().sql_marshall(),
            ("S0", "S1", None, None, "T0", "T1", 1, None),
        )

    def test_sql_primary_key(self) -> None:
        self.assertTupleEqual(self.get_entity().sql_primary_key(), (1,))

    def test_sql_unmarshall(self) -> None:
        t = Transfer.sql_unmarshall((1, "S0", "S1", None, None, "T0", "T1", 1, None))

        self.assertEqual(t.id, 1)
        self.assertEqual(t.from_stop_id, "S0")
        self.assertEqual(t.to_stop_id, "S1")
        self.assertEqual(t.from_route_id, "")
        self.assertEqual(t.to_route_id, "")
        self.assertEqual(t.from_trip_id, "T0")
        self.assertEqual(t.to_trip_id, "T1")
        self.assertEqual(t.type, Transfer.Type.TIMED)
        self.assertIsNone(t.min_transfer_time)
