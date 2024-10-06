from typing import Type

from impuls.model import Translation

from .template_entity import AbstractTestEntity


class TestTranslation(AbstractTestEntity.Template[Translation]):
    def get_entity(self) -> Translation:
        return Translation(
            table_name="stops",
            field_name="stop_name",
            language="pl",
            translation="Królewiec",
            record_id="1",
            id=42,
            extra_fields_json=None,
        )

    def get_type(self) -> Type[Translation]:
        return Translation

    def test_sql_marshall(self) -> None:
        self.assertTupleEqual(
            self.get_entity().sql_marshall(),
            ("stops", "stop_name", "pl", "Królewiec", "1", "", "", None),
        )

    def test_sql_primary_key(self) -> None:
        self.assertTupleEqual(self.get_entity().sql_primary_key(), (42,))

    def test_sql_unmarshall(self) -> None:
        t = Translation.sql_unmarshall(
            (42, "stops", "stop_name", "pl", "Królewiec", "1", "", "", None),
        )

        self.assertEqual(t.id, 42)
        self.assertEqual(t.table_name, "stops")
        self.assertEqual(t.field_name, "stop_name")
        self.assertEqual(t.language, "pl")
        self.assertEqual(t.translation, "Królewiec")
        self.assertEqual(t.record_id, "1")
        self.assertEqual(t.record_sub_id, "")
        self.assertEqual(t.field_value, "")
        self.assertIsNone(t.extra_fields_json)
