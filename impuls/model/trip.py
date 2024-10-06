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
class Trip(Entity, ExtraFieldsMixin):
    """Trips represent a single journey made by a vehicle, belonging to
    a specific :py:class:`Route` and :py:class:`Calendar`, grouping multiple
    :py:class:`StopTime` objects.

    Equivalent to `GTFS's trips.txt entries <https://gtfs.org/schedule/reference/#tripstxt>`_.
    """

    class Direction(IntEnum):
        OUTBOUND = 0
        INBOUND = 1

    id: str
    route_id: str
    calendar_id: str
    headsign: str = field(default="")
    short_name: str = field(default="", repr=False)
    direction: Optional[Direction] = field(default=None, repr=False)

    block_id: str = field(default="", repr=False)
    """block_id is used to group multiple trips where a rider can transfer without
    leaving a vehicle. This should only be used for circular routes or through service
    between routes; grouping multiple outbound and inbound trips (from a single diagram)
    of a single route with block_id provides no value to riders and creates visual confusion
    in consumer applications.

    Empty string maps to SQL NULL.
    """

    shape_id: str = field(default="", repr=False)
    """shape_id references :py:attr:`Shape.id`, with empty string mapping to SQL NULL."""

    wheelchair_accessible: Optional[bool] = field(default=None, repr=False)
    bikes_allowed: Optional[bool] = field(default=None, repr=False)
    exceptional: Optional[bool] = field(default=None, repr=False)
    extra_fields_json: Optional[str] = field(default=None, repr=False)

    @staticmethod
    def sql_table_name() -> LiteralString:
        return "trips"

    @staticmethod
    def sql_create_table() -> LiteralString:
        return """CREATE TABLE trips (
            trip_id TEXT PRIMARY KEY,
            route_id TEXT NOT NULL REFERENCES routes(route_id) ON DELETE CASCADE ON UPDATE CASCADE,
            calendar_id TEXT NOT NULL REFERENCES calendars(calendar_id)
                ON DELETE CASCADE ON UPDATE CASCADE,
            headsign TEXT NOT NULL DEFAULT '',
            short_name TEXT NOT NULL DEFAULT '',
            direction INTEGER DEFAULT NULL CHECK (direction IN (0, 1)),
            block_id TEXT DEFAULT NULL,
            shape_id TEXT DEFAULT NULL REFERENCES shapes(shape_id)
                ON DELETE CASCADE ON UPDATE CASCADE,
            wheelchair_accessible INTEGER DEFAULT NULL CHECK (wheelchair_accessible IN (0, 1)),
            bikes_allowed INTEGER DEFAULT NULL CHECK (bikes_allowed IN (0, 1)),
            exceptional INTEGER DEFAULT NULL CHECK (exceptional IN (0, 1)),
            extra_fields_json TEXT DEFAULT NULL
        ) STRICT;
        CREATE INDEX idx_trips_route_id ON trips(route_id);
        CREATE INDEX idx_trips_calendar_id ON trips(calendar_id);
        CREATE INDEX idx_trips_block_id ON trips(block_id);
        CREATE INDEX idx_trips_shape_id ON trips(shape_id);"""

    @staticmethod
    def sql_columns() -> LiteralString:
        return (
            "(trip_id, route_id, calendar_id, headsign, short_name, direction, block_id, "
            "shape_id, wheelchair_accessible, bikes_allowed, exceptional, extra_fields_json)"
        )

    @staticmethod
    def sql_placeholder() -> LiteralString:
        return "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"

    @staticmethod
    def sql_where_clause() -> LiteralString:
        return "trip_id = ?"

    @staticmethod
    def sql_set_clause() -> LiteralString:
        return (
            "trip_id = ?, route_id = ?, calendar_id = ?, headsign = ?, short_name = ?, "
            "direction = ?, block_id = ?, shape_id = ?, wheelchair_accessible = ?, "
            "bikes_allowed = ?, exceptional = ?, extra_fields_json = ?"
        )

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
            self.extra_fields_json,
        )

    def sql_primary_key(self) -> tuple[SQLNativeType, ...]:
        return (self.id,)

    @classmethod
    def sql_unmarshall(cls: TypeOf[Self], row: Sequence[SQLNativeType]) -> Self:
        return cls(
            **DataclassSQLBuilder(row)
            .field("id", str)
            .field("route_id", str)
            .field("calendar_id", str)
            .field("headsign", str)
            .field("short_name", str)
            .nullable_field("direction", int, cls.Direction)
            .optional_field("block_id", str, lambda x: x or "")
            .optional_field("shape_id", str, lambda x: x or "")
            .nullable_field("wheelchair_accessible", bool)
            .nullable_field("bikes_allowed", bool)
            .nullable_field("exceptional", bool)
            .nullable_field("extra_fields_json", str)
            .kwargs()
        )
