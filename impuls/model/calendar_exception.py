from dataclasses import dataclass, field
from enum import IntEnum
from typing import Mapping, Sequence
from typing import Type as TypeOf
from typing import final

from typing_extensions import LiteralString

from ..tools.types import Self, SQLNativeType
from .meta import DataclassGTFSBuilder, DataclassSQLBuilder, ImpulsBase
from .utility_types import Date


@final
@dataclass(unsafe_hash=True)
class CalendarException(ImpulsBase):
    class Type(IntEnum):
        ADDED = 1
        REMOVED = 2

    calendar_id: str = field(compare=True)
    date: Date = field(compare=False)
    exception_type: Type = field(compare=False, repr=False)

    @staticmethod
    def gtfs_table_name() -> LiteralString:
        return "calendar_dates"

    def gtfs_marshall(self) -> dict[str, str]:
        return {
            "service_id": self.calendar_id,
            "date": self.date.strftime("%Y%m%d"),
            "exception_type": str(self.exception_type.value),
        }

    @classmethod
    def gtfs_unmarshall(cls: TypeOf[Self], row: Mapping[str, str]) -> Self:
        return cls(
            **DataclassGTFSBuilder(row)
            .field("calendar_id", "service_id")
            .field("date", "date", Date.from_ymd_str)
            .field("exception_type", "exception_type", lambda x: cls.Type(int(x)))
            .kwargs()
        )

    @staticmethod
    def sql_table_name() -> LiteralString:
        return "calendar_exceptions"

    @staticmethod
    def sql_create_table() -> LiteralString:
        return """CREATE TABLE calendar_exceptions (
            calendar_id TEXT NOT NULL REFERENCES calendars(calendar_id),
            date TEXT NOT NULL CHECK (date LIKE '____-__-__'),
            exception_type INTEGER NOT NULL CHECK (exception_type IN (1, 2)),
            PRIMARY KEY (calendar_id, date)
        ) STRICT;"""

    @staticmethod
    def sql_placeholder() -> LiteralString:
        return "(?, ?, ?)"

    @staticmethod
    def sql_where_clause() -> LiteralString:
        return "id = ? AND date = ?"

    def sql_marshall(self) -> tuple[SQLNativeType, ...]:
        return (
            self.calendar_id,
            str(self.date),
            self.exception_type.value,
        )

    @classmethod
    def sql_unmarshall(cls: TypeOf[Self], row: Sequence[SQLNativeType]) -> Self:
        return cls(
            **DataclassSQLBuilder(row)
            .field("calendar_id", str)
            .field("date", str, Date.from_ymd_str)
            .field("exception_type", int, lambda x: cls.Type(x))
            .kwargs()
        )
