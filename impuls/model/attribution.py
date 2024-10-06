# © Copyright 2022-2024 Mikołaj Kuranowski
# SPDX-License-Identifier: GPL-3.0-or-later

from dataclasses import dataclass, field
from typing import Optional, Sequence
from typing import Type as TypeOf
from typing import final

from typing_extensions import LiteralString

from ..tools.types import Self, SQLNativeType
from .meta.entity import Entity
from .meta.extra_fields_mixin import ExtraFieldsMixin
from .meta.sql_builder import DataclassSQLBuilder


@final
@dataclass
class Attribution(Entity, ExtraFieldsMixin):
    """Attribution represents a copyright or any other attribution which must be attached
    to the dataset.

    Equivalent to `GTFS's attributions.txt entries <https://gtfs.org/schedule/reference/#attributionstxt>`_.
    """  # noqa: E501

    id: str
    organization_name: str
    is_producer: bool = field(default=False, repr=False)
    is_operator: bool = field(default=False, repr=False)
    is_authority: bool = field(default=False, repr=False)
    is_data_source: bool = field(default=False, repr=False)
    url: str = field(default="", repr=False)
    email: str = field(default="", repr=False)
    phone: str = field(default="", repr=False)
    extra_fields_json: Optional[str] = field(default=None, repr=False)

    @staticmethod
    def sql_table_name() -> LiteralString:
        return "attributions"

    @staticmethod
    def sql_create_table() -> LiteralString:
        return """CREATE TABLE attributions (
            attribution_id TEXT PRIMARY KEY,
            organization_name TEXT NOT NULL,
            is_producer INTEGER NOT NULL CHECK (is_producer IN (0, 1)),
            is_operator INTEGER NOT NULL CHECK (is_operator IN (0, 1)),
            is_authority INTEGER NOT NULL CHECK (is_authority IN (0, 1)),
            is_data_source INTEGER NOT NULL CHECK (is_data_source IN (0, 1)),
            url TEXT NOT NULL DEFAULT '',
            email TEXT NOT NULL DEFAULT '',
            phone TEXT NOT NULL DEFAULT '',
            extra_fields_json TEXT DEFAULT NULL
        ) STRICT;"""

    @staticmethod
    def sql_columns() -> LiteralString:
        return (
            "(attribution_id, organization_name, is_producer, is_operator, is_authority, "
            "is_data_source, url, email, phone, extra_fields_json)"
        )

    @staticmethod
    def sql_placeholder() -> LiteralString:
        return "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"

    @staticmethod
    def sql_where_clause() -> LiteralString:
        return "attribution_id = ?"

    @staticmethod
    def sql_set_clause() -> LiteralString:
        return (
            "attribution_id = ?, organization_name = ?, is_producer = ?, is_operator = ?, "
            "is_authority = ?, is_data_source = ?, url = ?, email = ?, phone = ?, "
            "extra_fields_json = ?"
        )

    def sql_marshall(self) -> tuple[SQLNativeType, ...]:
        return (
            self.id,
            self.organization_name,
            int(self.is_producer),
            int(self.is_operator),
            int(self.is_authority),
            int(self.is_data_source),
            self.url,
            self.email,
            self.phone,
            self.extra_fields_json,
        )

    def sql_primary_key(self) -> tuple[SQLNativeType, ...]:
        return (self.id,)

    @classmethod
    def sql_unmarshall(cls: TypeOf[Self], row: Sequence[SQLNativeType]) -> Self:
        return cls(
            **DataclassSQLBuilder(row)
            .field("id", str)
            .field("organization_name", str)
            .field("is_producer", bool)
            .field("is_operator", bool)
            .field("is_authority", bool)
            .field("is_data_source", bool)
            .field("url", str)
            .field("email", str)
            .field("phone", str)
            .nullable_field("extra_fields_json", str)
            .kwargs()
        )
