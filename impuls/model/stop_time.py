from dataclasses import dataclass, field
from enum import IntEnum
from typing import Mapping, Optional, Sequence
from typing import Type as TypeOf
from typing import final

from typing_extensions import LiteralString

from ..tools.types import Self, SQLNativeType
from .meta.entity import Entity
from .meta.gtfs_builder import DataclassGTFSBuilder
from .meta.sql_builder import DataclassSQLBuilder
from .meta.utility_types import TimePoint


@final
@dataclass(unsafe_hash=True)
class StopTime(Entity):
    class PassengerExchange(IntEnum):
        SCHEDULED_STOP = 0
        NONE = 1
        MUST_PHONE = 2
        ON_REQUEST = 3

    trip_id: str = field(compare=True)
    stop_id: str = field(compare=False)
    stop_sequence: int = field(compare=True, repr=False)
    arrival_time: TimePoint = field(compare=False, repr=False)
    departure_time: TimePoint = field(compare=False, repr=False)

    pickup_type: PassengerExchange = field(
        default=PassengerExchange.SCHEDULED_STOP, compare=False, repr=False
    )

    drop_off_type: PassengerExchange = field(
        default=PassengerExchange.SCHEDULED_STOP, compare=False, repr=False
    )

    stop_headsign: str = field(default="", compare=False, repr=False)
    shape_dist_traveled: Optional[float] = field(default=None, compare=False, repr=False)
    original_stop_id: str = field(default="", compare=False, repr=False)

    @staticmethod
    def gtfs_table_name() -> LiteralString:
        return "stop_times"

    def gtfs_marshall(self) -> dict[str, str]:
        return {
            "trip_id": self.trip_id,
            "stop_id": self.stop_id,
            "stop_sequence": str(self.stop_sequence),
            "arrival_time": str(self.arrival_time),
            "departure_time": str(self.departure_time),
            "pickup_type": str(self.pickup_type.value),
            "drop_off_type": str(self.drop_off_type.value),
            "stop_headsign": self.stop_headsign,
            "shape_dist_traveled": (
                str(self.shape_dist_traveled) if self.shape_dist_traveled is not None else ""
            ),
            "original_stop_id": self.original_stop_id,
        }

    @classmethod
    def gtfs_unmarshall(cls: TypeOf[Self], row: Mapping[str, str]) -> Self:
        return cls(
            **DataclassGTFSBuilder(row)
            .field("trip_id", "trip_id")
            .field("stop_id", "stop_id")
            .field("stop_sequence", "stop_sequence", int)
            .field("arrival_time", "arrival_time", TimePoint.from_str)
            .field("departure_time", "departure_time", TimePoint.from_str)
            .field(
                "pickup_type",
                "pickup_type",
                lambda x: cls.PassengerExchange(int(x)),
                fallback_value=cls.PassengerExchange.SCHEDULED_STOP,
            )
            .field(
                "drop_off_type",
                "drop_off_type",
                lambda x: cls.PassengerExchange(int(x)),
                fallback_value=cls.PassengerExchange.SCHEDULED_STOP,
            )
            .field("stop_headsign", "stop_headsign", fallback_value="")
            .field(
                "shape_dist_traveled",
                "shape_dist_traveled",
                lambda x: float(x) if x else None,
                fallback_value=None,
            )
            .kwargs()
        )

    @staticmethod
    def sql_table_name() -> LiteralString:
        return "stop_times"

    @staticmethod
    def sql_create_table() -> LiteralString:
        return """CREATE TABLE stop_times (
            trip_id TEXT NOT NULL,
            stop_id TEXT NOT NULL REFERENCES stops(stop_id) ON DELETE CASCADE ON UPDATE CASCADE,
            stop_sequence INTEGER NOT NULL CHECK (stop_sequence >= 0),
            arrival_time INTEGER NOT NULL,
            departure_time INTEGER NOT NULL,
            pickup_type INTEGER NOT NULL DEFAULT 0 CHECK (pickup_type IN (0, 1, 2, 3)),
            drop_off_type INTEGER NOT NULL DEFAULT 0 CHECK (drop_off_type IN (0, 1, 2, 3)),
            stop_headsign TEXT NOT NULL DEFAULT '',
            shape_dist_traveled REAL DEFAULT NULL,
            original_stop_id TEXT NOT NULL DEFAULT '',
            PRIMARY KEY (trip_id, stop_sequence)
        ) STRICT;
        CREATE INDEX idx_stop_times_stop_id ON stop_times(stop_id);"""

    @staticmethod
    def sql_placeholder() -> LiteralString:
        return "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"

    @staticmethod
    def sql_where_clause() -> LiteralString:
        return "trip_id = ? AND stop_sequence = ?"

    @staticmethod
    def sql_set_clause() -> LiteralString:
        return (
            "trip_id = ?, stop_id = ?, stop_sequence = ?, arrival_time = ?, departure_time = ?, "
            "pickup_type = ?, drop_off_type = ?, stop_headsign = ?, shape_dist_traveled = ?, "
            "original_stop_id = ?"
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
            .kwargs()
        )
