from dataclasses import dataclass, field
from enum import IntEnum
from typing import Mapping, Optional, Sequence
from typing import Type as TypeOf
from typing import final

from typing_extensions import LiteralString

from ..tools.types import Self, SQLNativeType
from .meta.entity import Entity
from .meta.gtfs_builder import (
    DataclassGTFSBuilder,
    from_optional_bool_empty_none,
    from_optional_bool_zero_none,
    to_optional_bool_empty_none,
    to_optional_bool_zero_none,
)
from .meta.sql_builder import DataclassSQLBuilder


@final
@dataclass(unsafe_hash=True)
class Trip(Entity):
    class Direction(IntEnum):
        OUTBOUND = 0
        INBOUND = 1

    id: str = field(compare=True)
    route_id: str = field(compare=False)
    calendar_id: str = field(compare=False)
    headsign: str = field(default="", compare=False)
    short_name: str = field(default="", compare=False, repr=False)
    direction: Optional[Direction] = field(default=None, compare=False, repr=False)

    # NOTE: block_id is a special case when serialized to SQL;
    #       the empty string is mapped to NULL.
    #       This makes it easier to treat it as a key of some sorts.
    block_id: str = field(default="", compare=False, repr=False)

    # NOTE: shape_id is a special case when serialized to SQL;
    #       the empty string is mapped to NULL.
    #       This makes it easier to treat it as a key of some sorts.
    shape_id: str = field(default="", compare=False, repr=False)

    wheelchair_accessible: Optional[bool] = field(default=None, compare=False, repr=False)
    bikes_allowed: Optional[bool] = field(default=None, compare=False, repr=False)
    exceptional: Optional[bool] = field(default=None, compare=False, repr=False)

    @staticmethod
    def gtfs_table_name() -> LiteralString:
        return "trips"

    def gtfs_marshall(self) -> dict[str, str]:
        return {
            "trip_id": self.id,
            "route_id": self.route_id,
            "service_id": self.calendar_id,
            "trip_headsign": self.headsign,
            "trip_short_name": self.short_name,
            "direction_id": str(self.direction.value) if self.direction is not None else "",
            "block_id": self.block_id,
            "shape_id": self.shape_id,
            "wheelchair_accessible": from_optional_bool_zero_none(self.wheelchair_accessible),
            "bikes_allowed": from_optional_bool_zero_none(self.bikes_allowed),
            "exceptional": from_optional_bool_empty_none(self.exceptional),
        }

    @classmethod
    def gtfs_unmarshall(cls: TypeOf[Self], row: Mapping[str, str]) -> Self:
        return cls(
            **DataclassGTFSBuilder(row)
            .field("id", "trip_id")
            .field("route_id", "route_id")
            .field("calendar_id", "service_id")
            .field("headsign", "trip_headsign", fallback_value="")
            .field("short_name", "trip_short_name", fallback_value="")
            .field("direction", "direction_id", lambda x: cls.Direction(int(x)) if x else None)
            .field("block_id", "block_id")
            .field("shape_id", "shape_id")
            .field(
                "wheelchair_accessible",
                "wheelchair_accessible",
                to_optional_bool_zero_none,
                fallback_value=None,
            )
            .field(
                "bikes_allowed",
                "bikes_allowed",
                to_optional_bool_zero_none,
                fallback_value=None,
            )
            .field("exceptional", "exceptional", to_optional_bool_empty_none, fallback_value=None)
            .kwargs()
        )

    @staticmethod
    def sql_table_name() -> LiteralString:
        return "trips"

    @staticmethod
    def sql_create_table() -> LiteralString:
        return """CREATE TABLE trips (
            trip_id TEXT PRIMARY KEY,
            route_id TEXT REFERENCES routes(route_id) NOT NULL,
            calendar_id TEXT REFERENCES calendars(calendar_id) NOT NULL,
            headsign TEXT NOT NULL DEFAULT '',
            short_name TEXT NOT NULL DEFAULT '',
            direction INTEGER DEFAULT NULL CHECK (direction IN (0, 1)),
            block_id TEXT DEFAULT NULL,
            shape_id TEXT DEFAULT NULL,
            wheelchair_accessible INTEGER DEFAULT NULL CHECK (wheelchair_accessible IN (0, 1)),
            bikes_allowed INTEGER DEFAULT NULL CHECK (bikes_allowed IN (0, 1)),
            exceptional INTEGER DEFAULT NULL CHECK (exceptional IN (0, 1))
        ) STRICT;
        CREATE INDEX idx_trips_route_id ON trips(route_id);
        CREATE INDEX idx_trips_calendar_id ON trips(calendar_id);
        CREATE INDEX idx_trips_block_id ON trips(block_id);
        CREATE INDEX idx_trips_shape_id ON trips(shape_id);"""

    @staticmethod
    def sql_placeholder() -> LiteralString:
        return "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"

    @staticmethod
    def sql_where_clause() -> LiteralString:
        return "trip_id = ?"

    def sql_marshall(self) -> tuple[SQLNativeType, ...]:
        return (
            self.id,
            self.route_id,
            self.calendar_id,
            self.headsign,
            self.short_name,
            self.direction.value if self.direction is not None else None,
            self.block_id or None,
            self.shape_id or None,
            int(self.wheelchair_accessible) if self.wheelchair_accessible is not None else None,
            int(self.bikes_allowed) if self.bikes_allowed is not None else None,
            int(self.exceptional) if self.exceptional is not None else None,
        )

    @classmethod
    def sql_unmarshall(cls: TypeOf[Self], row: Sequence[SQLNativeType]) -> Self:
        return cls(
            **DataclassSQLBuilder(row)
            .field("id", str)
            .field("route_id", str)
            .field("calendar_id", str)
            .field("headsign", str)
            .field("short_name", str)
            .field("direction", int, cls.Direction, nullable=True)
            .field("block_id", Optional[str], lambda x: x or "")
            .field("shape_id", Optional[str], lambda x: x or "")
            .field("wheelchair_accessible", bool, nullable=True)
            .field("bikes_allowed", bool, nullable=True)
            .field("exceptional", bool, nullable=True)
            .kwargs()
        )
