from dataclasses import dataclass, field
from typing import Mapping, Sequence
from typing import Type as TypeOf
from typing import final

from typing_extensions import LiteralString

from ..tools.types import Self, SQLNativeType
from .meta.entity import Entity
from .meta.gtfs_builder import DataclassGTFSBuilder, to_bool_allow_empty
from .meta.sql_builder import DataclassSQLBuilder
from .meta.utility_types import TimePoint


@final
@dataclass
class Frequency(Entity):
    trip_id: str
    start_time: TimePoint
    end_time: TimePoint = field(repr=False)
    headway: int
    exact_times: bool = field(default=False, repr=False)

    @staticmethod
    def gtfs_table_name() -> LiteralString:
        return "frequencies"

    def gtfs_marshall(self) -> dict[str, str]:
        return {
            "trip_id": self.trip_id,
            "start_time": str(self.start_time),
            "end_time": str(self.end_time),
            "headway_secs": str(self.headway),
            "exact_times": "1" if self.exact_times else "0",
        }

    @classmethod
    def gtfs_unmarshall(cls: TypeOf[Self], row: Mapping[str, str]) -> Self:
        return cls(
            **DataclassGTFSBuilder(row)
            .field("trip_id")
            .field("start_time", converter=TimePoint.from_str)
            .field("end_time", converter=TimePoint.from_str)
            .field("headway", "headway_secs", int)
            .field("exact_times", converter=to_bool_allow_empty)
            .kwargs()
        )

    @staticmethod
    def sql_table_name() -> LiteralString:
        return "frequencies"

    @staticmethod
    def sql_create_table() -> LiteralString:
        return """CREATE TABLE frequencies (
            trip_id TEXT NOT NULL REFERENCES trips(trip_id) ON DELETE CASCADE ON UPDATE CASCADE,
            start_time INTEGER NOT NULL,
            end_time INTEGER NOT NULL,
            headway INTEGER NOT NULL CHECK (headway > 0),
            exact_times INTEGER DEFAULT 0 CHECK (exact_times IN (0, 1)),
            PRIMARY KEY (trip_id, start_time)
        ) STRICT;"""

    @staticmethod
    def sql_columns() -> LiteralString:
        return "(trip_id, start_time, end_time, headway, exact_times)"

    @staticmethod
    def sql_placeholder() -> LiteralString:
        return "(?, ?, ?, ?, ?)"

    @staticmethod
    def sql_where_clause() -> LiteralString:
        return "trip_id = ? AND start_time = ?"

    @staticmethod
    def sql_set_clause() -> LiteralString:
        return "trip_id = ?, start_time = ?, end_time = ?, headway = ?, exact_times = ?"

    def sql_marshall(self) -> tuple[SQLNativeType, ...]:
        return (
            self.trip_id,
            int(self.start_time.total_seconds()),
            int(self.end_time.total_seconds()),
            self.headway,
            int(self.exact_times),
        )

    def sql_primary_key(self) -> tuple[SQLNativeType, ...]:
        return (self.trip_id, int(self.start_time.total_seconds()))

    @classmethod
    def sql_unmarshall(cls: TypeOf[Self], row: Sequence[SQLNativeType]) -> Self:
        return cls(
            **DataclassSQLBuilder(row)
            .field("trip_id", str)
            .field("start_time", int, lambda x: TimePoint(seconds=x))
            .field("end_time", int, lambda x: TimePoint(seconds=x))
            .field("headway", int)
            .field("exact_times", bool)
            .kwargs()
        )
