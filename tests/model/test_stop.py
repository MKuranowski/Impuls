from typing import Type, final

from impuls.model import Stop

from .template_entity import AbstractTestEntity


@final
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
            pkpplk_code="",
            ibnr_code="",
        )

    def get_type(self) -> Type[Stop]:
        return Stop

    def test_gtfs_marshall(self) -> None:
        self.assertDictEqual(
            self.get_entity().gtfs_marshall(),
            {
                "stop_id": "0",
                "stop_name": "Foo",
                "stop_lat": "50.847",
                "stop_lon": "4.383",
                "stop_code": "S0",
                "zone_id": "",
                "location_type": "0",
                "parent_station": "",
                "wheelchair_boarding": "1",
                "platform_code": "",
                "stop_pkpplk": "",
                "stop_IBNR": "",
            },
        )

    def test_gtfs_marshall_parent_station(self) -> None:
        s = self.get_entity()
        s.parent_station = "1"
        d = s.gtfs_marshall()

        self.assertEqual(d["parent_station"], "1")

    def test_gtfs_unmarshall(self) -> None:
        s = Stop.gtfs_unmarshall(
            {
                "stop_id": "0",
                "stop_name": "Foo",
                "stop_lat": "50.847",
                "stop_lon": "4.383",
                "stop_code": "S0",
                "zone_id": "",
                "location_type": "0",
                "parent_station": "",
                "wheelchair_boarding": "1",
                "platform_code": "",
                "stop_pkpplk": "",
                "stop_IBNR": "",
            }
        )

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
        self.assertEqual(s.pkpplk_code, "")
        self.assertEqual(s.ibnr_code, "")

    def test_gtfs_unmarshall_parent_station(self) -> None:
        s = Stop.gtfs_unmarshall(
            {
                "stop_id": "0",
                "stop_name": "Foo",
                "stop_lat": "50.847",
                "stop_lon": "4.383",
                "stop_code": "S0",
                "zone_id": "",
                "location_type": "0",
                "parent_station": "1",
                "wheelchair_boarding": "1",
                "platform_code": "",
                "stop_pkpplk": "",
                "stop_IBNR": "",
            }
        )

        self.assertEqual(s.parent_station, "1")

    def test_sql_marshall(self) -> None:
        self.assertTupleEqual(
            self.get_entity().sql_marshall(),
            ("0", "Foo", 50.847, 4.383, "S0", "", 0, None, 1, "", "", ""),
        )

    def test_sql_marshall_parent_station(self) -> None:
        s = self.get_entity()
        s.parent_station = "1"

        self.assertTupleEqual(
            s.sql_marshall(),
            ("0", "Foo", 50.847, 4.383, "S0", "", 0, "1", 1, "", "", ""),
        )

    def test_sql_primary_key(self) -> None:
        self.assertTupleEqual(self.get_entity().sql_primary_key(), ("0",))

    def test_sql_unmarshall(self) -> None:
        s = Stop.sql_unmarshall(("0", "Foo", 50.847, 4.383, "S0", "", 0, None, 1, "", "", ""))

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
        self.assertEqual(s.pkpplk_code, "")
        self.assertEqual(s.ibnr_code, "")

    def test_sql_unmarshall_parent_station(self) -> None:
        s = Stop.sql_unmarshall(("0", "Foo", 50.847, 4.383, "S0", "", 0, "1", 1, "", "", ""))
        self.assertEqual(s.parent_station, "1")
