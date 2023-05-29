from typing import Type, final

from impuls.model import Agency

from .template_entity import TestEntity


@final
class TestAgency(TestEntity.Template[Agency]):
    def get_entity(self) -> Agency:
        return Agency(
            id="0",
            name="Foo",
            url="https://example.com/",
            timezone="Europe/Brussels",
            lang="en",
            phone="",
            fare_url="",
        )

    def get_type(self) -> Type[Agency]:
        return Agency

    def test_gtfs_marshall(self) -> None:
        self.assertDictEqual(
            self.get_entity().gtfs_marshall(),
            {
                "agency_id": "0",
                "agency_name": "Foo",
                "agency_url": "https://example.com/",
                "agency_timezone": "Europe/Brussels",
                "agency_lang": "en",
                "agency_phone": "",
                "agency_fare_url": "",
            },
        )

    def test_gtfs_unmarshall(self) -> None:
        a = Agency.gtfs_unmarshall(
            {
                "agency_id": "0",
                "agency_name": "Foo",
                "agency_url": "https://example.com/",
                "agency_timezone": "Europe/Brussels",
                "agency_lang": "en",
                "agency_phone": "",
                "agency_fare_url": "",
            }
        )

        self.assertEqual(a.id, "0")
        self.assertEqual(a.name, "Foo")
        self.assertEqual(a.url, "https://example.com/")
        self.assertEqual(a.timezone, "Europe/Brussels")
        self.assertEqual(a.lang, "en")
        self.assertEqual(a.phone, "")
        self.assertEqual(a.fare_url, "")

    def test_sql_marshall(self) -> None:
        self.assertTupleEqual(
            self.get_entity().sql_marshall(),
            ("0", "Foo", "https://example.com/", "Europe/Brussels", "en", "", ""),
        )

    def test_sql_primary_key(self) -> None:
        self.assertTupleEqual(self.get_entity().sql_primary_key(), ("0",))

    def test_sql_unmarshall(self) -> None:
        a = Agency.sql_unmarshall(
            (
                "0",
                "Foo",
                "https://example.com/",
                "Europe/Brussels",
                "en",
                "",
                "",
            )
        )

        self.assertEqual(a.id, "0")
        self.assertEqual(a.name, "Foo")
        self.assertEqual(a.url, "https://example.com/")
        self.assertEqual(a.timezone, "Europe/Brussels")
        self.assertEqual(a.lang, "en")
        self.assertEqual(a.phone, "")
        self.assertEqual(a.fare_url, "")
