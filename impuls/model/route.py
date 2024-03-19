from dataclasses import dataclass, field
from enum import IntEnum
from typing import Optional, Sequence
from typing import Type as TypeOf
from typing import final

from typing_extensions import LiteralString

from ..tools.types import Self, SQLNativeType
from .meta.entity import Entity
from .meta.sql_builder import DataclassSQLBuilder


@final
@dataclass
class Route(Entity):
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
            sort_order INTEGER
        ) STRICT;
        CREATE INDEX idx_routes_agency_id ON routes(agency_id);"""

    @staticmethod
    def sql_columns() -> LiteralString:
        return "(route_id, agency_id, short_name, long_name, type, color, text_color, sort_order)"

    @staticmethod
    def sql_placeholder() -> LiteralString:
        return "(?, ?, ?, ?, ?, ?, ?, ?)"

    @staticmethod
    def sql_where_clause() -> LiteralString:
        return "route_id = ?"

    @staticmethod
    def sql_set_clause() -> LiteralString:
        return (
            "route_id = ?, agency_id = ?, short_name = ?, long_name = ?, type = ?, "
            "color = ?, text_color = ?, sort_order = ?"
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
            .field("sort_order", int, nullable=True)
            .kwargs()
        )
