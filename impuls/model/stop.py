from dataclasses import dataclass, field
from enum import IntEnum
from typing import Mapping, Optional, Sequence
from typing import Type as TypeOf
from typing import final

from typing_extensions import LiteralString

from ..tools.types import Self, SQLNativeType
from .meta import DataclassGTFSBuilder, DataclassSQLBuilder, ImpulsBase


@final
@dataclass(unsafe_hash=True)
class Stop(ImpulsBase):
    class LocationType(IntEnum):
        STOP = 0
        STATION = 1
        EXIT = 2

    # TODO: Add wheelchair_boarding

    id: str = field(compare=True)
    name: str = field(compare=False)
    lat: float = field(compare=False, repr=False)
    lon: float = field(compare=False, repr=False)
    code: str = field(default="", compare=False)
    zone_id: str = field(default="", compare=False, repr=False)
    location_type: LocationType = field(default=LocationType.STOP, compare=False, repr=False)
    parent_station: str = field(default="", compare=False, repr=False)
    platform_code: str = field(default="", compare=False, repr=False)
    pkpplk_code: str = field(default="", compare=False, repr=False)
    ibnr_code: str = field(default="", compare=False, repr=False)

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
                lambda x: cls.LocationType(int(x)),
                fallback_value=cls.LocationType.STOP,
            )
            .field("parent_station", "parent_station", fallback_value="")
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
            platform_code TEXT NOT NULL DEFAULT '',
            pkpplk_code TEXT NOT NULL DEFAULT '',
            ibnr_code TEXT NOT NULL DEFAULT ''
        ) STRICT;
        CREATE INDEX idx_stops_zone ON stops(zone_id);
        CREATE INDEX idx_stops_parent_station ON stops(parent_station);"""

    @staticmethod
    def sql_placeholder() -> LiteralString:
        return "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"

    @staticmethod
    def sql_where_clause() -> LiteralString:
        return "stop_id = ?"

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
            self.platform_code,
            self.pkpplk_code,
            self.ibnr_code,
        )

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
            .field("parent_station", Optional[str], lambda x: x or "")
            .field("platform_code", str)
            .field("pkpplk_code", str)
            .field("ibnr_code", str)
            .kwargs()
        )