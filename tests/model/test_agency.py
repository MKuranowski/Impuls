from typing import Type

from impuls.model import Agency

from .template_entity import AbstractTestEntity


class TestAgency(AbstractTestEntity.Template[Agency]):
    def get_entity(self) -> Agency:
        return Agency(
            id="0",
            name="Foo",
            url="https://example.com/",
            timezone="Europe/Brussels",
            lang="en",
            phone="",
            fare_url="",
            extra_fields_json=r'{"agency_email":"foo@example.com"}',
        )

    def get_type(self) -> Type[Agency]:
        return Agency

    def test_sql_marshall(self) -> None:
        self.assertTupleEqual(
            self.get_entity().sql_marshall(),
            (
                "0",
                "Foo",
                "https://example.com/",
                "Europe/Brussels",
                "en",
                "",
                "",
                r'{"agency_email":"foo@example.com"}',
            ),
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
                r'{"agency_email":"foo@example.com"}',
            )
        )

        self.assertEqual(a.id, "0")
        self.assertEqual(a.name, "Foo")
        self.assertEqual(a.url, "https://example.com/")
        self.assertEqual(a.timezone, "Europe/Brussels")
        self.assertEqual(a.lang, "en")
        self.assertEqual(a.phone, "")
        self.assertEqual(a.fare_url, "")
        self.assertEqual(a.extra_fields_json, r'{"agency_email":"foo@example.com"}')
        self.assertDictEqual(a.get_extra_fields(), {"agency_email": "foo@example.com"})
