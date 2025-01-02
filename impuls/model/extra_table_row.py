# © Copyright 2024-2025 Mikołaj Kuranowski
# SPDX-License-Identifier: GPL-3.0-or-later

import json
from dataclasses import dataclass
from typing import Mapping, Optional, Sequence, final

from typing_extensions import LiteralString

from ..tools.types import Self, SQLNativeType
from .meta.entity import Entity
from .meta.sql_builder import DataclassSQLBuilder


@final
@dataclass
class ExtraTableRow(Entity):
    """ExtraTableRow is a special :py:class:`~impuls.model.Entity` which allows
    defining extra tables and their rows in a generic way.

    Note that by default, :py:class:`~impuls.tasks.LoadGTFS` does not load unknown
    tables and columns.
    """

    id: int
    """This field is ignored on :py:meth:`DBConnection.create` -
    SQLite automatically generates an ID.
    """

    table_name: str
    fields_json: str = r"{}"
    row_sort_order: Optional[int] = None

    @staticmethod
    def sql_table_name() -> LiteralString:
        return "extra_table_rows"

    @staticmethod
    def sql_create_table() -> LiteralString:
        return r"""CREATE TABLE extra_table_rows (
            extra_table_row_id INTEGER PRIMARY KEY,
            table_name TEXT NOT NULL,
            fields_json TEXT NOT NULL DEFAULT '{}',
            row_sort_order INTEGER
        ) STRICT;
        CREATE INDEX idx_extra_table_rows_table_row ON
            extra_table_rows(table_name, row_sort_order);
        """

    @staticmethod
    def sql_columns() -> LiteralString:
        return "(table_name, fields_json, row_sort_order)"

    @staticmethod
    def sql_placeholder() -> LiteralString:
        return "(?, ?, ?)"

    @staticmethod
    def sql_where_clause() -> LiteralString:
        return "extra_table_row_id = ?"

    @staticmethod
    def sql_set_clause() -> LiteralString:
        return "table_name = ?, fields_json = ?, row_sort_order = ?"

    def sql_marshall(self) -> tuple[SQLNativeType, ...]:
        return (
            self.table_name,
            self.fields_json,
            self.row_sort_order,
        )

    def sql_primary_key(self) -> tuple[SQLNativeType, ...]:
        return (self.id,)

    @classmethod
    def sql_unmarshall(cls, row: Sequence[SQLNativeType]) -> Self:
        return cls(
            **DataclassSQLBuilder(row)
            .field("id", int)
            .field("table_name", str)
            .field("fields_json", str)
            .nullable_field("row_sort_order", int)
            .kwargs()
        )

    def get_fields(self) -> dict[str, str]:
        """get_fields returns a fresh dictionary of all fields stored in the
        :py:attr:`fields_json`.
        """
        return json.loads(self.fields_json)

    def set_fields(self, fields: Mapping[str, str]) -> None:
        """set_fields sets all fields in :py:attr:`fields_json` from the
        provided mapping.
        """
        self.fields_json = json.dumps(fields, indent=None, separators=(",", ":"))

    def get_field(self, field: str) -> Optional[str]:
        """get_field returns a specific field stored in :py:attr:`fields_json`.

        Invoking this function causes an unconditional parse of :py:attr:`fields_json`,
        which, if called repeatedly, may incur a performance penalty.
        Use :py:meth:`get_fields` to avoid parsing overhead.
        """
        return self.get_fields().get(field)

    def set_field(self, field: str, value: Optional[str]) -> None:
        """set_field sets a specific field stored in :py:attr:`extra_fields_json`.

        Invoking this function causes an unconditional parse and serialization of
        :py:attr:`fields_json`. Use :py:meth:`get_fields` and
        :py:meth:`set_fields` once to avoid JSON serialization overhead.
        """
        fields = self.get_fields()
        if value is None:
            fields.pop(field, None)
        else:
            fields[field] = value
        self.set_fields(fields)
