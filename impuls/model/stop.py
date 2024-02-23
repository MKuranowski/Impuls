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
    from_optional_bool_zero_none,
    to_optional_bool_zero_none,
)
from .meta.sql_builder import DataclassSQLBuilder


@final
@dataclass
class Stop(Entity):
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

    # NOTE: parent_station is a special case when serialized to SQL;
    #       the empty string is mapped to NULL.
    #       This makes it easier to treat it as a key referencing Stop.id.
    parent_station: str = field(default="", repr=False)

    wheelchair_boarding: Optional[bool] = field(default=None, repr=False)
    platform_code: str = field(default="", repr=False)
    pkpplk_code: str = field(default="", repr=False)
    ibnr_code: str = field(default="", repr=False)

    @staticmethod
    def gtfs_table_name() -> LiteralString:
        return "stops"

    def gtfs_marshall(self) -> dict[str, str]:
        return {
            "stop_id": self.id,
            "stop_name": self.name,
            "stop_lat": str(self.lat),
            "stop_lon": str(self.lon),
            "stop_code": self.code,
            "zone_id": self.zone_id,
            "location_type": str(self.location_type.value),
            "parent_station": self.parent_station,
            "wheelchair_boarding": from_optional_bool_zero_none(self.wheelchair_boarding),
            "platform_code": self.platform_code,
            "stop_pkpplk": self.pkpplk_code,
            "stop_IBNR": self.ibnr_code,
        }

    @classmethod
    def gtfs_unmarshall(cls: TypeOf[Self], row: Mapping[str, str]) -> Self:
        return cls(
            **DataclassGTFSBuilder(row)
            .field("id", "stop_id")
            .field("name", "stop_name")
            .field("lat", "stop_lat", float)
            .field("lon", "stop_lon", float)
            .field("code", "stop_code", fallback_value="")
            .field("zone_id", "zone_id", fallback_value="")
            .field(
                "location_type",
                "location_type",
                lambda x: cls.LocationType(int(x)) if x else cls.LocationType.STOP,
                fallback_value=cls.LocationType.STOP,
            )
            .field("parent_station", "parent_station", fallback_value="")
            .field(
                "wheelchair_boarding",
                "wheelchair_boarding",
                to_optional_bool_zero_none,
                fallback_value=None,
            )
            .field("platform_code", "platform_code", fallback_value="")
            .field("pkpplk_code", "stop_pkpplk", fallback_value="")
            .field("ibnr_code", "stop_IBNR", fallback_value="")
            .kwargs()
        )

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
            pkpplk_code TEXT NOT NULL DEFAULT '',
            ibnr_code TEXT NOT NULL DEFAULT ''
        ) STRICT;
        CREATE INDEX idx_stops_zone ON stops(zone_id);
        CREATE INDEX idx_stops_parent_station ON stops(parent_station);"""

    @staticmethod
    def sql_columns() -> LiteralString:
        return (
            "(stop_id, name, lat, lon, code, zone_id, location_type, parent_station, "
            "wheelchair_boarding, platform_code, pkpplk_code, ibnr_code)"
        )

    @staticmethod
    def sql_placeholder() -> LiteralString:
        return "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"

    @staticmethod
    def sql_where_clause() -> LiteralString:
        return "stop_id = ?"

    @staticmethod
    def sql_set_clause() -> LiteralString:
        return (
            "stop_id = ?, name = ?, lat = ?, lon = ?, code = ?, zone_id = ?, location_type = ?, "
            "parent_station = ?, wheelchair_boarding = ?, platform_code = ?, pkpplk_code = ?, "
            "ibnr_code = ?"
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
            self.pkpplk_code,
            self.ibnr_code,
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
            .field("parent_station", Optional[str], lambda x: x or "")  # type: ignore
            .field("wheelchair_boarding", bool, nullable=True)
            .field("platform_code", str)
            .field("pkpplk_code", str)
            .field("ibnr_code", str)
            .kwargs()
        )
