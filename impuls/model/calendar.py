# © Copyright 2022-2024 Mikołaj Kuranowski
# SPDX-License-Identifier: GPL-3.0-or-later

from dataclasses import dataclass, field
from functools import cached_property
from typing import Optional, Sequence
from typing import Type as TypeOf
from typing import final

from typing_extensions import LiteralString

from ..tools.temporal import date_range
from ..tools.types import Self, SQLNativeType
from .meta.entity import Entity
from .meta.extra_fields_mixin import ExtraFieldsMixin
from .meta.sql_builder import DataclassSQLBuilder
from .meta.utility_types import Date


@final
@dataclass
class Calendar(Entity, ExtraFieldsMixin):
    """Calendar defines a set of dates on which :py:class:`Trip` instances operate.

    Equivalent to `GTFS's calendar.txt entries <https://gtfs.org/schedule/reference/#calendartxt>`_.

    Contrary to GTFS, Calendar entries are mandatory, even if all operating dates are defined
    using :py:class:`CalendarException` instances. If this is the case, all weekdays should be set
    to ``False`` and :py:attr:`start_date` and :py:attr:`end_date` should be set to
    :py:const:`Date.SIGNALS_EXCEPTIONS`.
    """  # noqa: E501

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
    extra_fields_json: Optional[str] = field(default=None, repr=False)

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
            desc TEXT NOT NULL DEFAULT '',
            extra_fields_json TEXT DEFAULT NULL
        ) STRICT;"""

    @staticmethod
    def sql_columns() -> LiteralString:
        return (
            "(calendar_id, monday, tuesday, wednesday, thursday, friday, saturday, sunday, "
            "start_date, end_date, desc, extra_fields_json)"
        )

    @staticmethod
    def sql_placeholder() -> LiteralString:
        return "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"

    @staticmethod
    def sql_where_clause() -> LiteralString:
        return "calendar_id = ?"

    @staticmethod
    def sql_set_clause() -> LiteralString:
        return (
            "calendar_id = ?, monday = ?, tuesday = ?, wednesday = ?, thursday = ?, "
            "friday = ?, saturday = ?, sunday = ?, start_date = ?, end_date = ?, desc = ?, "
            "extra_fields_json = ?"
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
            self.extra_fields_json,
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
            .nullable_field("extra_fields_json", str)
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

        Use :py:meth:`CalendarException.reflect_in_active_dates`
        to take :py:class:`CalendarException` instances into account.
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
