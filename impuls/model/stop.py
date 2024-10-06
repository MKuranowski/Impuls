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
class Stop(Entity, ExtraFieldsMixin):
    """Stop can represent 3 different point-like entities,
    depending on the :py:attr:`location_type` value, usually physical stops.

    :py:obj:`LocationType.STOP` represent physical places where passengers can embark
    and disembark from vehicles.

    :py:obj:`LocationType.STATION` represent a grouping of multiple stops and exits under
    a single physical structure. Note that two stops on an opposite side of a road do not
    form a station (as these do not form a single physical structure), but an underground
    bus terminus might. Stop-station structures should be only used when you want to provide
    exits, or different platform positions. If those details are not available,
    it is ok to provide single stops representing entire railway stations.

    :py:obj:`LocationType.EXIT` represent an exit to a station.

    Equivalent to `GTFS's stops.txt entries <https://gtfs.org/schedule/reference/#stopstxt>`_.
    """

    class LocationType(IntEnum):
        STOP = 0
        STATION = 1
        EXIT = 2

    id: str
    name: str
    lat: float
    lon: float
    code: str = field(default="")
    zone_id: str = field(default="", repr=False)
    location_type: LocationType = field(default=LocationType.STOP, repr=False)

    parent_station: str = field(default="", repr=False)
    """parent_station references :py:attr:`Stop.id`, with empty string mapping to SQL NULL.
    Optional for stops, forbidden for stations and mandatory for exits.
    """

    wheelchair_boarding: Optional[bool] = field(default=None, repr=False)
    platform_code: str = field(default="", repr=False)
    extra_fields_json: Optional[str] = field(default=None, repr=False)

    @staticmethod
    def sql_table_name() -> LiteralString:
        return "stops"

    @staticmethod
    def sql_create_table() -> LiteralString:
        return """CREATE TABLE stops (
            stop_id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            lat REAL NOT NULL,
            lon REAL NOT NULL,
            code TEXT NOT NULL DEFAULT '',
            zone_id TEXT NOT NULL DEFAULT '',
            location_type INTEGER NOT NULL DEFAULT 0 CHECK (location_type IN (0, 1, 2)),
            parent_station TEXT REFERENCES stops(stop_id) ON DELETE CASCADE ON UPDATE CASCADE,
            wheelchair_boarding INTEGER DEFAULT NULL CHECK (wheelchair_boarding IN (0, 1)),
            platform_code TEXT NOT NULL DEFAULT '',
            extra_fields_json TEXT DEFAULT NULL
        ) STRICT;
        CREATE INDEX idx_stops_zone ON stops(zone_id);
        CREATE INDEX idx_stops_parent_station ON stops(parent_station);"""

    @staticmethod
    def sql_columns() -> LiteralString:
        return (
            "(stop_id, name, lat, lon, code, zone_id, location_type, parent_station, "
            "wheelchair_boarding, platform_code, extra_fields_json)"
        )

    @staticmethod
    def sql_placeholder() -> LiteralString:
        return "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"

    @staticmethod
    def sql_where_clause() -> LiteralString:
        return "stop_id = ?"

    @staticmethod
    def sql_set_clause() -> LiteralString:
        return (
            "stop_id = ?, name = ?, lat = ?, lon = ?, code = ?, zone_id = ?, location_type = ?, "
            "parent_station = ?, wheelchair_boarding = ?, platform_code = ?, extra_fields_json = ?"
        )

    def sql_marshall(self) -> tuple[SQLNativeType, ...]:
        return (
            self.id,
            self.name,
            self.lat,
            self.lon,
            self.code,
            self.zone_id,
            int(self.location_type),
            self.parent_station or None,
            int(self.wheelchair_boarding) if self.wheelchair_boarding is not None else None,
            self.platform_code,
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
            .field("lat", float)
            .field("lon", float)
            .field("code", str)
            .field("zone_id", str)
            .field("location_type", int, lambda x: cls.LocationType(x))
            .optional_field("parent_station", str, lambda x: x or "")
            .nullable_field("wheelchair_boarding", bool)
            .field("platform_code", str)
            .nullable_field("extra_fields_json", str)
            .kwargs()
        )
