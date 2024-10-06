from typing import Type

from impuls.model import Stop

from .template_entity import AbstractTestEntity


class TestStop(AbstractTestEntity.Template[Stop]):
    def get_entity(self) -> Stop:
        return Stop(
            id="0",
            name="Foo",
            lat=50.847,
            lon=4.383,
            code="S0",
            zone_id="",
            location_type=Stop.LocationType.STOP,
            parent_station="",
            wheelchair_boarding=True,
            platform_code="",
            extra_fields_json=None,
        )

    def get_type(self) -> Type[Stop]:
        return Stop

    def test_sql_marshall(self) -> None:
        self.assertTupleEqual(
            self.get_entity().sql_marshall(),
            ("0", "Foo", 50.847, 4.383, "S0", "", 0, None, 1, "", None),
        )

    def test_sql_marshall_parent_station(self) -> None:
        s = self.get_entity()
        s.parent_station = "1"

        self.assertTupleEqual(
            s.sql_marshall(),
            ("0", "Foo", 50.847, 4.383, "S0", "", 0, "1", 1, "", None),
        )

    def test_sql_primary_key(self) -> None:
        self.assertTupleEqual(self.get_entity().sql_primary_key(), ("0",))

    def test_sql_unmarshall(self) -> None:
        s = Stop.sql_unmarshall(("0", "Foo", 50.847, 4.383, "S0", "", 0, None, 1, "", None))

        self.assertEqual(s.id, "0")
        self.assertEqual(s.name, "Foo")
        self.assertEqual(s.lat, 50.847)
        self.assertEqual(s.lon, 4.383)
        self.assertEqual(s.code, "S0")
        self.assertEqual(s.zone_id, "")
        self.assertEqual(s.location_type, Stop.LocationType.STOP)
        self.assertEqual(s.parent_station, "")
        self.assertEqual(s.wheelchair_boarding, True)
        self.assertEqual(s.platform_code, "")
        self.assertIsNone(s.extra_fields_json)

    def test_sql_unmarshall_parent_station(self) -> None:
        s = Stop.sql_unmarshall(("0", "Foo", 50.847, 4.383, "S0", "", 0, "1", 1, "", None))
        self.assertEqual(s.parent_station, "1")
