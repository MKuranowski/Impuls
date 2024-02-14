from dataclasses import dataclass, field
from functools import cached_property
from typing import Mapping, Sequence
from typing import Type as TypeOf
from typing import final

from typing_extensions import LiteralString

from ..tools.temporal import date_range
from ..tools.types import Self, SQLNativeType
from .meta.entity import Entity
from .meta.gtfs_builder import DataclassGTFSBuilder, from_bool, to_bool
from .meta.sql_builder import DataclassSQLBuilder
from .meta.utility_types import Date


@final
@dataclass
class Calendar(Entity):
    id: str
    monday: bool = field(default=False, repr=False)
    tuesday: bool = field(default=False, repr=False)
    wednesday: bool = field(default=False, repr=False)
    thursday: bool = field(default=False, repr=False)
    friday: bool = field(default=False, repr=False)
    saturday: bool = field(default=False, repr=False)
    sunday: bool = field(default=False, repr=False)
    start_date: Date = field(default=Date.SIGNALS_EXCEPTIONS)
    end_date: Date = field(default=Date.SIGNALS_EXCEPTIONS)
    desc: str = field(default="", repr=False)

    @staticmethod
    def gtfs_table_name() -> LiteralString:
        return "calendar"

    def gtfs_marshall(self) -> dict[str, str]:
        return {
            "service_id": self.id,
            "monday": from_bool(self.monday),
            "tuesday": from_bool(self.tuesday),
            "wednesday": from_bool(self.wednesday),
            "thursday": from_bool(self.thursday),
            "friday": from_bool(self.friday),
            "saturday": from_bool(self.saturday),
            "sunday": from_bool(self.sunday),
            "start_date": self.start_date.strftime("%Y%m%d"),
            "end_date": self.end_date.strftime("%Y%m%d"),
            "service_desc": self.desc,
        }

    @classmethod
    def gtfs_unmarshall(cls: TypeOf[Self], row: Mapping[str, str]) -> Self:
        return cls(
            **DataclassGTFSBuilder(row)
            .field("id", "service_id")
            .field("monday", "monday", to_bool)
            .field("tuesday", "tuesday", to_bool)
            .field("wednesday", "wednesday", to_bool)
            .field("thursday", "thursday", to_bool)
            .field("friday", "friday", to_bool)
            .field("saturday", "saturday", to_bool)
            .field("sunday", "sunday", to_bool)
            .field("start_date", "start_date", Date.from_ymd_str)
            .field("end_date", "end_date", Date.from_ymd_str)
            .field("desc", "service_desc", fallback_value="")
            .kwargs()
        )

    @staticmethod
    def sql_table_name() -> LiteralString:
        return "calendars"

    @staticmethod
    def sql_create_table() -> LiteralString:
        return """CREATE TABLE calendars (
            calendar_id TEXT PRIMARY KEY,
            monday INTEGER NOT NULL DEFAULT 0 CHECK (monday IN (0, 1)),
            tuesday INTEGER NOT NULL DEFAULT 0 CHECK (monday IN (0, 1)),
            wednesday INTEGER NOT NULL DEFAULT 0 CHECK (monday IN (0, 1)),
            thursday INTEGER NOT NULL DEFAULT 0 CHECK (monday IN (0, 1)),
            friday INTEGER NOT NULL DEFAULT 0 CHECK (monday IN (0, 1)),
            saturday INTEGER NOT NULL DEFAULT 0 CHECK (monday IN (0, 1)),
            sunday INTEGER NOT NULL DEFAULT 0 CHECK (monday IN (0, 1)),
            start_date TEXT NOT NULL DEFAULT '1111-11-11' CHECK (start_date LIKE '____-__-__'),
            end_date TEXT NOT NULL DEFAULT '1111-11-11' CHECK (end_date LIKE '____-__-__'),
            desc TEXT NOT NULL DEFAULT ''
        ) STRICT;"""

    @staticmethod
    def sql_placeholder() -> LiteralString:
        return "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"

    @staticmethod
    def sql_where_clause() -> LiteralString:
        return "calendar_id = ?"

    @staticmethod
    def sql_set_clause() -> LiteralString:
        return (
            "calendar_id = ?, monday = ?, tuesday = ?, wednesday = ?, thursday = ?, "
            "friday = ?, saturday = ?, sunday = ?, start_date = ?, end_date = ?, desc = ?"
        )

    def sql_marshall(self) -> tuple[SQLNativeType, ...]:
        return (
            self.id,
            int(self.monday),
            int(self.tuesday),
            int(self.wednesday),
            int(self.thursday),
            int(self.friday),
            int(self.saturday),
            int(self.sunday),
            str(self.start_date),
            str(self.end_date),
            self.desc,
        )

    def sql_primary_key(self) -> tuple[SQLNativeType, ...]:
        return (self.id,)

    @classmethod
    def sql_unmarshall(cls: TypeOf[Self], row: Sequence[SQLNativeType]) -> Self:
        return cls(
            **DataclassSQLBuilder(row)
            .field("id", str)
            .field("monday", bool)
            .field("tuesday", bool)
            .field("wednesday", bool)
            .field("thursday", bool)
            .field("friday", bool)
            .field("saturday", bool)
            .field("sunday", bool)
            .field("start_date", str, Date.from_ymd_str)
            .field("end_date", str, Date.from_ymd_str)
            .field("desc", str)
            .kwargs()
        )

    @cached_property
    def compressed_weekdays(self) -> int:
        return (
            self.monday
            | (self.tuesday << 1)
            | (self.wednesday << 2)
            | (self.thursday << 3)
            | (self.friday << 4)
            | (self.saturday << 5)
            | (self.sunday << 6)
        )

    def compute_active_dates(self) -> set[Date]:
        """Computes the set of active dates of this Calendar,
        **not** taking exceptions into account.

        Use CalendarException.reflect_in_active_dates
        to take CalendarExceptions into account.
        """
        if self.start_date == Date.SIGNALS_EXCEPTIONS and self.end_date == Date.SIGNALS_EXCEPTIONS:
            return set()

        if self.compressed_weekdays == 0:
            return set()

        return {
            date
            for date in date_range(self.start_date, self.end_date)
            if self.compressed_weekdays & (1 << date.weekday())
        }
