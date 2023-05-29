from typing import Type, final

from impuls.model import Attribution

from .template_entity import TestEntity


@final
class TestAttribution(TestEntity.Template[Attribution]):
    def get_entity(self) -> Attribution:
        return Attribution(
            id="0",
            organization_name="Foo",
            is_producer=True,
            is_operator=False,
            is_authority=True,
            is_data_source=True,
            url="https://example.com/",
            email="",
            phone="",
        )

    def get_type(self) -> Type[Attribution]:
        return Attribution

    def test_gtfs_marshall(self) -> None:
        self.assertDictEqual(
            self.get_entity().gtfs_marshall(),
            {
                "attribution_id": "0",
                "organization_name": "Foo",
                "is_producer": "1",
                "is_operator": "0",
                "is_authority": "1",
                "is_data_source": "1",
                "attribution_url": "https://example.com/",
                "attribution_email": "",
                "attribution_phone": "",
            },
        )

    def test_gtfs_unmarshall(self) -> None:
        a = Attribution.gtfs_unmarshall(
            {
                "attribution_id": "0",
                "organization_name": "Foo",
                "is_producer": "1",
                "is_operator": "0",
                "is_authority": "1",
                "is_data_source": "1",
                "attribution_url": "https://example.com/",
                "attribution_email": "",
                "attribution_phone": "",
            }
        )

        self.assertEqual(a.id, "0")
        self.assertEqual(a.organization_name, "Foo")
        self.assertEqual(a.is_producer, True)
        self.assertEqual(a.is_operator, False)
        self.assertEqual(a.is_authority, True)
        self.assertEqual(a.is_data_source, True)
        self.assertEqual(a.url, "https://example.com/")
        self.assertEqual(a.email, "")
        self.assertEqual(a.phone, "")

    def test_sql_marshall(self) -> None:
        self.assertTupleEqual(
            self.get_entity().sql_marshall(),
            ("0", "Foo", 1, 0, 1, 1, "https://example.com/", "", ""),
        )

    def test_sql_primary_key(self) -> None:
        self.assertTupleEqual(self.get_entity().sql_primary_key(), ("0",))

    def test_sql_unmarshall(self) -> None:
        a = Attribution.sql_unmarshall(("0", "Foo", 1, 0, 1, 1, "https://example.com/", "", ""))

        self.assertEqual(a.id, "0")
        self.assertEqual(a.organization_name, "Foo")
        self.assertEqual(a.is_producer, True)
        self.assertEqual(a.is_operator, False)
        self.assertEqual(a.is_authority, True)
        self.assertEqual(a.is_data_source, True)
        self.assertEqual(a.url, "https://example.com/")
        self.assertEqual(a.email, "")
        self.assertEqual(a.phone, "")
