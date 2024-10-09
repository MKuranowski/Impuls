# © Copyright 2024 Mikołaj Kuranowski
# SPDX-License-Identifier: GPL-3.0-or-later

from typing import Type

from impuls.model import ExtraTableRow

from .template_entity import AbstractTestEntity


class TestExtraTableRow(AbstractTestEntity.Template[ExtraTableRow]):
    def get_entity(self) -> ExtraTableRow:
        return ExtraTableRow(
            id=1,
            table_name="cities",
            fields_json=r'{"city_id":"0","city_name":"Warszawa"}',
        )

    def get_type(self) -> Type[ExtraTableRow]:
        return ExtraTableRow

    def test_sql_marshall(self) -> None:
        self.assertTupleEqual(
            self.get_entity().sql_marshall(),
            ("cities", r'{"city_id":"0","city_name":"Warszawa"}', None),
        )

    def test_sql_primary_key(self) -> None:
        self.assertTupleEqual(self.get_entity().sql_primary_key(), (1,))

    def test_sql_unmarshall(self) -> None:
        e = ExtraTableRow.sql_unmarshall(
            (1, "cities", r'{"city_id":"0","city_name":"Warszawa"}', None),
        )

        self.assertEqual(e.id, 1)
        self.assertEqual(e.table_name, "cities")
        self.assertEqual(e.fields_json, r'{"city_id":"0","city_name":"Warszawa"}')
        self.assertIsNone(e.row_sort_order)

    def test_get_fields(self) -> None:
        self.assertDictEqual(
            self.get_entity().get_fields(),
            {"city_id": "0", "city_name": "Warszawa"},
        )

    def test_set_fields(self) -> None:
        e = self.get_entity()
        e.set_fields({"city_id": "1", "city_name": "Modlin"})
        self.assertEqual(
            e.fields_json,
            r'{"city_id":"1","city_name":"Modlin"}',
        )

    def test_get_field(self) -> None:
        self.assertEqual(self.get_entity().get_field("city_id"), "0")

    def test_get_field_missing(self) -> None:
        self.assertIsNone(self.get_entity().get_field("city_country"))

    def test_set_field(self) -> None:
        e = self.get_entity()
        e.set_field("country_code", "pl")
        self.assertEqual(
            e.fields_json,
            r'{"city_id":"0","city_name":"Warszawa","country_code":"pl"}',
        )

    def test_set_field_overwrites(self) -> None:
        e = self.get_entity()
        e.set_field("city_name", "Warsaw")
        self.assertEqual(e.fields_json, r'{"city_id":"0","city_name":"Warsaw"}')

    def test_set_field_none(self) -> None:
        e = self.get_entity()
        e.set_field("city_name", None)
        self.assertEqual(e.fields_json, r'{"city_id":"0"}')
