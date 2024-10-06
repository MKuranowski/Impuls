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
from .meta.utility_types import Date


@final
@dataclass
class FeedInfo(Entity, ExtraFieldsMixin):
    """FeedInfo describes metadata about the schedule dataset.

    Equivalent to `GTFS's feed_info.txt <https://gtfs.org/schedule/reference/#feed_infotxt>`_.
    """

    publisher_name: str
    publisher_url: str = field(repr=False)
    lang: str = field()
    version: str = field(default="")
    contact_email: str = field(default="", repr=False)
    contact_url: str = field(default="", repr=False)
    start_date: Optional[Date] = field(default=None)
    end_date: Optional[Date] = field(default=None)
    extra_fields_json: Optional[str] = field(default=None, repr=False)

    id: int = field(default=0, repr=False)
    """id of the FeedInfo must be always 0, as there can only be
    at most one entry in the feed_info table."""

    @staticmethod
    def sql_table_name() -> LiteralString:
        return "feed_info"

    @staticmethod
    def sql_create_table() -> LiteralString:
        return """CREATE TABLE feed_info (
            feed_info_id INTEGER PRIMARY KEY CHECK (feed_info_id = '0'),
            publisher_name TEXT NOT NULL,
            publisher_url TEXT NOT NULL,
            lang TEXT NOT NULL,
            version TEXT NOT NULL DEFAULT '',
            contact_email TEXT NOT NULL DEFAULT '',
            contact_url TEXT NOT NULL DEFAULT '',
            start_date TEXT DEFAULT NULL CHECK (start_date LIKE '____-__-__'),
            end_date TEXT DEFAULT NULL CHECK (end_date LIKE '____-__-__'),
            extra_fields_json TEXT DEFAULT NULL
        ) STRICT;"""

    @staticmethod
    def sql_columns() -> LiteralString:
        return (
            "(feed_info_id, publisher_name, publisher_url, lang, version, contact_email, "
            "contact_url, start_date, end_date, extra_fields_json)"
        )

    @staticmethod
    def sql_placeholder() -> LiteralString:
        return "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"

    @staticmethod
    def sql_where_clause() -> LiteralString:
        return "feed_info_id = ?"

    @staticmethod
    def sql_set_clause() -> LiteralString:
        return (
            "feed_info_id = ?, publisher_name = ?, publisher_url = ?, lang = ?, "
            "version = ?, contact_email = ?, contact_url = ?, start_date = ?, end_date = ?, "
            "extra_fields_json = ?"
        )

    def sql_marshall(self) -> tuple[SQLNativeType, ...]:
        return (
            self.id,
            self.publisher_name,
            self.publisher_url,
            self.lang,
            self.version,
            self.contact_email,
            self.contact_url,
            str(self.start_date) if self.start_date else None,
            str(self.end_date) if self.end_date else None,
            self.extra_fields_json,
        )

    def sql_primary_key(self) -> tuple[SQLNativeType, ...]:
        return (self.id,)

    @classmethod
    def sql_unmarshall(cls: TypeOf[Self], row: Sequence[SQLNativeType]) -> Self:
        return cls(
            **DataclassSQLBuilder(row)
            .field("id", int)
            .field("publisher_name", str)
            .field("publisher_url", str)
            .field("lang", str)
            .field("version", str)
            .field("contact_email", str)
            .field("contact_url", str)
            .nullable_field("start_date", str, Date.from_ymd_str)
            .nullable_field("end_date", str, Date.from_ymd_str)
            .nullable_field("extra_fields_json", str)
            .kwargs()
        )
