# © Copyright 2022-2024 Mikołaj Kuranowski
# SPDX-License-Identifier: GPL-3.0-or-later

from dataclasses import dataclass
from enum import IntEnum
from typing import Iterable, Sequence
from typing import Type as TypeOf
from typing import final

from typing_extensions import LiteralString

from ..tools.types import Self, SQLNativeType
from .meta.entity import Entity
from .meta.sql_builder import DataclassSQLBuilder
from .meta.utility_types import Date


@final
@dataclass
class CalendarException(Entity):
    """CalendarExceptions are used to override operating dates defined by a :py:class:`Calendar`.

    Equivalent to `GTFS's calendar_dates.txt entries <https://gtfs.org/schedule/reference/#calendar_datestxt>`_.

    Contrary to GTFS, :py:class:`Calendar` entries are mandatory (even if empty), as
    :py:attr:`calendar_id` is **always** a foreign key referencing :py:attr:`Calendar.id`.
    """  # noqa: E501

    class Type(IntEnum):
        ADDED = 1
        REMOVED = 2

    calendar_id: str
    date: Date
    exception_type: Type

    @staticmethod
    def sql_table_name() -> LiteralString:
        return "calendar_exceptions"

    @staticmethod
    def sql_create_table() -> LiteralString:
        return """CREATE TABLE calendar_exceptions (
            calendar_id TEXT NOT NULL REFERENCES calendars(calendar_id)
                ON DELETE CASCADE ON UPDATE CASCADE,
            date TEXT NOT NULL CHECK (date LIKE '____-__-__'),
            exception_type INTEGER NOT NULL CHECK (exception_type IN (1, 2)),
            PRIMARY KEY (calendar_id, date)
        ) STRICT;"""

    @staticmethod
    def sql_columns() -> LiteralString:
        return "(calendar_id, date, exception_type)"

    @staticmethod
    def sql_placeholder() -> LiteralString:
        return "(?, ?, ?)"

    @staticmethod
    def sql_where_clause() -> LiteralString:
        return "calendar_id = ? AND date = ?"

    @staticmethod
    def sql_set_clause() -> LiteralString:
        return "calendar_id = ?, date = ?, exception_type = ?"

    def sql_marshall(self) -> tuple[SQLNativeType, ...]:
        return (
            self.calendar_id,
            str(self.date),
            self.exception_type.value,
        )

    def sql_primary_key(self) -> tuple[SQLNativeType, ...]:
        return (self.calendar_id, str(self.date))

    @classmethod
    def sql_unmarshall(cls: TypeOf[Self], row: Sequence[SQLNativeType]) -> Self:
        return cls(
            **DataclassSQLBuilder(row)
            .field("calendar_id", str)
            .field("date", str, Date.from_ymd_str)
            .field("exception_type", int, cls.Type)
            .kwargs()
        )

    @staticmethod
    def reflect_in_active_dates(
        active_dates: set[Date],
        exceptions: Iterable["CalendarException"],
    ) -> set[Date]:
        """Reflects a set of CalendarExceptions in a set of active dates.
        Warning! The provided set is both modified in-place and later returned.

        The set of active dates can come from Calendar.compute_active_dates.
        """
        for exception in exceptions:
            match exception.exception_type:
                case CalendarException.Type.ADDED:
                    active_dates.add(exception.date)
                case CalendarException.Type.REMOVED:
                    active_dates.discard(exception.date)
        return active_dates
