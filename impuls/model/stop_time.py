from dataclasses import dataclass, field
from enum import IntEnum
from typing import Optional, Sequence
from typing import Type as TypeOf
from typing import final

from typing_extensions import LiteralString

from ..tools.types import Self, SQLNativeType
from .meta.entity import Entity
from .meta.sql_builder import DataclassSQLBuilder
from .meta.utility_types import TimePoint


@final
@dataclass
class StopTime(Entity):
    class PassengerExchange(IntEnum):
        SCHEDULED_STOP = 0
        NONE = 1
        MUST_PHONE = 2
        ON_REQUEST = 3

    trip_id: str
    stop_id: str
    stop_sequence: int = field(repr=False)
    arrival_time: TimePoint = field(repr=False)
    departure_time: TimePoint = field(repr=False)
    pickup_type: PassengerExchange = field(default=PassengerExchange.SCHEDULED_STOP, repr=False)
    drop_off_type: PassengerExchange = field(default=PassengerExchange.SCHEDULED_STOP, repr=False)
    stop_headsign: str = field(default="", repr=False)
    shape_dist_traveled: Optional[float] = field(default=None, repr=False)
    original_stop_id: str = field(default="", repr=False)
    platform: str = field(default="", repr=False)

    @staticmethod
    def sql_table_name() -> LiteralString:
        return "stop_times"

    @staticmethod
    def sql_create_table() -> LiteralString:
        return """CREATE TABLE stop_times (
            trip_id TEXT NOT NULL REFERENCES trips(trip_id) ON DELETE CASCADE ON UPDATE CASCADE,
            stop_id TEXT NOT NULL REFERENCES stops(stop_id) ON DELETE CASCADE ON UPDATE CASCADE,
            stop_sequence INTEGER NOT NULL CHECK (stop_sequence >= 0),
            arrival_time INTEGER NOT NULL,
            departure_time INTEGER NOT NULL,
            pickup_type INTEGER NOT NULL DEFAULT 0 CHECK (pickup_type IN (0, 1, 2, 3)),
            drop_off_type INTEGER NOT NULL DEFAULT 0 CHECK (drop_off_type IN (0, 1, 2, 3)),
            stop_headsign TEXT NOT NULL DEFAULT '',
            shape_dist_traveled REAL DEFAULT NULL,
            original_stop_id TEXT NOT NULL DEFAULT '',
            platform TEXT NOT NULL DEFAULT '',
            PRIMARY KEY (trip_id, stop_sequence)
        ) STRICT;
        CREATE INDEX idx_stop_times_stop_id ON stop_times(stop_id);"""

    @staticmethod
    def sql_columns() -> LiteralString:
        return (
            "(trip_id, stop_id, stop_sequence, arrival_time, departure_time, pickup_type, "
            "drop_off_type, stop_headsign, shape_dist_traveled, original_stop_id, platform)"
        )

    @staticmethod
    def sql_placeholder() -> LiteralString:
        return "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"

    @staticmethod
    def sql_where_clause() -> LiteralString:
        return "trip_id = ? AND stop_sequence = ?"

    @staticmethod
    def sql_set_clause() -> LiteralString:
        return (
            "trip_id = ?, stop_id = ?, stop_sequence = ?, arrival_time = ?, departure_time = ?, "
            "pickup_type = ?, drop_off_type = ?, stop_headsign = ?, shape_dist_traveled = ?, "
            "original_stop_id = ?, platform = ?"
        )

    def sql_marshall(self) -> tuple[SQLNativeType, ...]:
        return (
            self.trip_id,
            self.stop_id,
            self.stop_sequence,
            int(self.arrival_time.total_seconds()),
            int(self.departure_time.total_seconds()),
            self.pickup_type.value,
            self.drop_off_type.value,
            self.stop_headsign,
            self.shape_dist_traveled,
            self.original_stop_id,
            self.platform,
        )

    def sql_primary_key(self) -> tuple[SQLNativeType, ...]:
        return (self.trip_id, self.stop_sequence)

    @classmethod
    def sql_unmarshall(cls: TypeOf[Self], row: Sequence[SQLNativeType]) -> Self:
        return cls(
            **DataclassSQLBuilder(row)
            .field("trip_id", str)
            .field("stop_id", str)
            .field("stop_sequence", int)
            .field("arrival_time", int, lambda x: TimePoint(seconds=x))
            .field("departure_time", int, lambda x: TimePoint(seconds=x))
            .field("pickup_type", int, cls.PassengerExchange)
            .field("drop_off_type", int, cls.PassengerExchange)
            .field("stop_headsign", str)
            .field("shape_dist_traveled", float, nullable=True)
            .field("original_stop_id", str)
            .field("platform", str)
            .kwargs()
        )
