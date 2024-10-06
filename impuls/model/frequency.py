# © Copyright 2022-2024 Mikołaj Kuranowski
# SPDX-License-Identifier: GPL-3.0-or-later

from dataclasses import dataclass, field
from typing import Optional, Sequence
from typing import Type as TypeOf
from typing import final

from typing_extensions import LiteralString

from ..tools.types import Self, SQLNativeType
from .meta.entity import Entity
from .meta.extra_fields_mixin import ExtraFieldsMixin
from .meta.sql_builder import DataclassSQLBuilder
from .meta.utility_types import TimePoint


@final
@dataclass
class Frequency(Entity, ExtraFieldsMixin):
    """Frequency instances provide an alternative way of defining multiple trips
    in bulk. When a :py:class:`Trip` has at least one :py:class:`Frequency`, that trips
    :py:class:`StopTime` absolute times are ignored, instead multiple trips using the relative
    time differences are used as a pattern for multiple trips following the same pattern.

    Equivalent to `GTFS's frequencies.txt entries <https://gtfs.org/schedule/reference/#frequenciestxt>`_.
    """  # noqa: E501

    trip_id: str
    start_time: TimePoint
    end_time: TimePoint = field(repr=False)
    headway: int
    exact_times: bool = field(default=False, repr=False)
    extra_fields_json: Optional[str] = field(default=None, repr=False)

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
            extra_fields_json TEXT DEFAULT NULL,
            PRIMARY KEY (trip_id, start_time)
        ) STRICT;"""

    @staticmethod
    def sql_columns() -> LiteralString:
        return "(trip_id, start_time, end_time, headway, exact_times, extra_fields_json)"

    @staticmethod
    def sql_placeholder() -> LiteralString:
        return "(?, ?, ?, ?, ?, ?)"

    @staticmethod
    def sql_where_clause() -> LiteralString:
        return "trip_id = ? AND start_time = ?"

    @staticmethod
    def sql_set_clause() -> LiteralString:
        return (
            "trip_id = ?, start_time = ?, end_time = ?, headway = ?, exact_times = ?, "
            "extra_fields_json = ?"
        )

    def sql_marshall(self) -> tuple[SQLNativeType, ...]:
        return (
            self.trip_id,
            int(self.start_time.total_seconds()),
            int(self.end_time.total_seconds()),
            self.headway,
            int(self.exact_times),
            self.extra_fields_json,
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
            .nullable_field("extra_fields_json", str)
            .kwargs()
        )
