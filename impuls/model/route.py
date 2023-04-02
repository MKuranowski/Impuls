from dataclasses import dataclass, field
from enum import IntEnum
from typing import Mapping, Optional, Sequence
from typing import Type as TypeOf
from typing import final

from typing_extensions import LiteralString

from ..tools.types import Self, SQLNativeType
from .meta.entity import Entity
from .meta.gtfs_builder import DataclassGTFSBuilder, to_optional
from .meta.sql_builder import DataclassSQLBuilder


@final
@dataclass(unsafe_hash=True)
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

    id: str = field(compare=True)
    agency_id: str = field(compare=False, repr=False)
    short_name: str = field(compare=False)
    long_name: str = field(compare=False)
    type: Type = field(compare=False)
    color: str = field(default="", compare=False, repr=False)
    text_color: str = field(default="", compare=False, repr=False)
    sort_order: Optional[int] = field(default=None, compare=False, repr=False)

    @staticmethod
    def gtfs_table_name() -> LiteralString:
        return "routes"

    def gtfs_marshall(self) -> dict[str, str]:
        return {
            "route_id": self.id,
            "agency_id": self.agency_id,
            "route_short_name": self.short_name,
            "route_long_name": self.long_name,
            "route_type": str(self.type.value),
            "route_color": self.color,
            "route_text_color": self.text_color,
            "route_sort_order": to_optional(self.sort_order),
        }

    @classmethod
    def gtfs_unmarshall(cls: TypeOf[Self], row: Mapping[str, str]) -> Self:
        return cls(
            **DataclassGTFSBuilder(row)
            .field("id", "route_id")
            .field("agency_id", "agency_id")
            .field("short_name", "route_short_name")
            .field("long_name", "route_long_name")
            .field("type", "route_type", lambda x: cls.Type(int(x)))
            .field("color", "route_color", fallback_value="")
            .field("text_color", "route_text_color", fallback_value="")
            .field("sort_order", "route_sort_order", lambda x: None if x == "" else int(x))
            .kwargs()
        )

    @staticmethod
    def sql_table_name() -> LiteralString:
        return "routes"

    @staticmethod
    def sql_create_table() -> LiteralString:
        return """CREATE TABLE routes (
            route_id TEXT PRIMARY KEY,
            agency_id TEXT REFERENCES agencies(agency_id) NOT NULL,
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
