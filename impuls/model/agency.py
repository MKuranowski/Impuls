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
class Agency(Entity, ExtraFieldsMixin):
    """Agency represents the entity/public body/company responsible for high-level management
    (especially fares) of a public transportation network.

    The exact meaning is up to the user, but an *Agency* should be the body riders associate
    as responsible for the transit system. For example, in Poland, for publicly run
    city networks this should be the city-run public transport authority (the organizer, e.g.
    Zarząd Transportu Miejskiego), but for train networks this should be the train company
    itself (e.g. Koleje Mazowieckie, even though technically the organizer is usually the
    voivodeship marshal).

    Equivalent to `GTFS's agency.txt entries <https://gtfs.org/schedule/reference/#agencytxt>`_.
    """

    id: str
    name: str
    url: str = field(repr=False)
    timezone: str = field(repr=False)
    lang: str = field(default="", repr=False)
    phone: str = field(default="", repr=False)
    fare_url: str = field(default="", repr=False)
    extra_fields_json: Optional[str] = field(default=None, repr=False)

    @staticmethod
    def sql_table_name() -> LiteralString:
        return "agencies"

    @staticmethod
    def sql_create_table() -> LiteralString:
        return """CREATE TABLE agencies (
            agency_id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            url TEXT NOT NULL,
            timezone TEXT NOT NULL,
            lang TEXT NOT NULL DEFAULT '',
            phone TEXT NOT NULL DEFAULT '',
            fare_url TEXT NOT NULL DEFAULT '',
            extra_fields_json TEXT DEFAULT NULL
        ) STRICT;"""

    @staticmethod
    def sql_columns() -> LiteralString:
        return "(agency_id, name, url, timezone, lang, phone, fare_url, extra_fields_json)"

    @staticmethod
    def sql_placeholder() -> LiteralString:
        return "(?, ?, ?, ?, ?, ?, ?, ?)"

    @staticmethod
    def sql_where_clause() -> LiteralString:
        return "agency_id = ?"

    @staticmethod
    def sql_set_clause() -> LiteralString:
        return (
            "agency_id = ?, name = ?, url = ?, timezone = ?, lang = ?, phone = ?, fare_url = ?, "
            "extra_fields_json = ?"
        )

    def sql_marshall(self) -> tuple[SQLNativeType, ...]:
        return (
            self.id,
            self.name,
            self.url,
            self.timezone,
            self.lang,
            self.phone,
            self.fare_url,
            self.extra_fields_json,
        )

    def sql_primary_key(self) -> tuple[SQLNativeType, ...]:
        return (self.id,)

    @classmethod
    def sql_unmarshall(cls: TypeOf[Self], row: Sequence[SQLNativeType]) -> Self:
        return cls(
            **DataclassSQLBuilder(row)
            .field("id", str)
            .field("name", str)
            .field("url", str)
            .field("timezone", str)
            .field("lang", str)
            .field("phone", str)
            .field("fare_url", str)
            .nullable_field("extra_fields_json", str)
            .kwargs()
        )
