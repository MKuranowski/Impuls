# © Copyright 2022-2024 Mikołaj Kuranowski
# SPDX-License-Identifier: GPL-3.0-or-later

from dataclasses import dataclass, field
from enum import IntEnum
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
class Route(Entity, ExtraFieldsMixin):
    """Route instances group multiple trips operated by one :py:class:`Agency` under
    a single, common identifier.

    The same as a "line"; not to be confused with a "shape" or a "pattern". For example
    all U2 services in Berlin should be grouped under a single route with short_name "U2" and
    long_name "Pankow - Ruhleben". For agencies where lines are not commonly used in passenger
    information, service types may be used instead (common use case for railway operators, e.g.
    PKP Intercity (Poland) should represent TLK, IC, EIC and EIP train categories as routes,
    and Korail (South Korea) should represent KTX, ITX, Nuriro, Mungunghwa and Saemeul
    train categories as routes). If there's no real distinction of services operated by
    an agency (common use case for long-haul coaches), a single route with agency name
    is sufficient.

    Equivalent to `GTFS's routes.txt entries <https://gtfs.org/schedule/reference/#routestxt>`_.
    """

    class Type(IntEnum):
        TRAM = 0
        METRO = 1
        RAIL = 2
        BUS = 3
        FERRY = 4
        CABLE_TRAM = 5
        GONDOLA = 6
        FUNICULAR = 7
        TROLLEYBUS = 11
        MONORAIL = 12

    id: str
    agency_id: str = field(repr=False)
    short_name: str
    long_name: str
    type: Type
    color: str = field(default="", repr=False)
    text_color: str = field(default="", repr=False)
    sort_order: Optional[int] = field(default=None, repr=False)
    extra_fields_json: Optional[str] = field(default=None, repr=False)

    @staticmethod
    def sql_table_name() -> LiteralString:
        return "routes"

    @staticmethod
    def sql_create_table() -> LiteralString:
        return """CREATE TABLE routes (
            route_id TEXT PRIMARY KEY,
            agency_id TEXT NOT NULL REFERENCES agencies(agency_id)
                ON DELETE CASCADE ON UPDATE CASCADE,
            short_name TEXT NOT NULL,
            long_name TEXT NOT NULL,
            type INTEGER NOT NULL CHECK (type IN (
                0, 1, 2, 3, 4, 5, 6, 7, 11, 12
            )),
            color TEXT NOT NULL DEFAULT '',
            text_color TEXT NOT NULL DEFAULT '',
            sort_order INTEGER,
            extra_fields_json TEXT DEFAULT NULL
        ) STRICT;
        CREATE INDEX idx_routes_agency_id ON routes(agency_id);"""

    @staticmethod
    def sql_columns() -> LiteralString:
        return (
            "(route_id, agency_id, short_name, long_name, type, color, text_color, sort_order, "
            "extra_fields_json)"
        )

    @staticmethod
    def sql_placeholder() -> LiteralString:
        return "(?, ?, ?, ?, ?, ?, ?, ?, ?)"

    @staticmethod
    def sql_where_clause() -> LiteralString:
        return "route_id = ?"

    @staticmethod
    def sql_set_clause() -> LiteralString:
        return (
            "route_id = ?, agency_id = ?, short_name = ?, long_name = ?, type = ?, "
            "color = ?, text_color = ?, sort_order = ?, extra_fields_json = ?"
        )

    def sql_marshall(self) -> tuple[SQLNativeType, ...]:
        return (
            self.id,
            self.agency_id,
            self.short_name,
            self.long_name,
            self.type.value,
            self.color,
            self.text_color,
            self.sort_order,
            self.extra_fields_json,
        )

    def sql_primary_key(self) -> tuple[SQLNativeType, ...]:
        return (self.id,)

    @classmethod
    def sql_unmarshall(cls: TypeOf[Self], row: Sequence[SQLNativeType]) -> Self:
        return cls(
            **DataclassSQLBuilder(row)
            .field("id", str)
            .field("agency_id", str)
            .field("short_name", str)
            .field("long_name", str)
            .field("type", int, cls.Type)
            .field("color", str)
            .field("text_color", str)
            .nullable_field("sort_order", int)
            .nullable_field("extra_fields_json", str)
            .kwargs()
        )
