from typing import Type

from impuls.model import Attribution

from .template_entity import AbstractTestEntity


class TestAttribution(AbstractTestEntity.Template[Attribution]):
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
            extra_fields_json=None,
        )

    def get_type(self) -> Type[Attribution]:
        return Attribution

    def test_sql_marshall(self) -> None:
        self.assertTupleEqual(
            self.get_entity().sql_marshall(),
            ("0", "Foo", 1, 0, 1, 1, "https://example.com/", "", "", None),
        )

    def test_sql_primary_key(self) -> None:
        self.assertTupleEqual(self.get_entity().sql_primary_key(), ("0",))

    def test_sql_unmarshall(self) -> None:
        a = Attribution.sql_unmarshall(
            ("0", "Foo", 1, 0, 1, 1, "https://example.com/", "", "", None),
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
        self.assertIsNone(a.extra_fields_json)
