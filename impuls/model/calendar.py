from dataclasses import dataclass, field
from typing import Mapping, Sequence
from typing import Type as TypeOf
from typing import final

from typing_extensions import LiteralString

from ..tools.types import Self, SQLNativeType
from .meta import DataclassGTFSBuilder, DataclassSQLBuilder, ImpulsBase
from .meta.gtfs_builder import from_bool, to_bool
from .utility_types import Date


@final
@dataclass(unsafe_hash=True)
class Calendar(ImpulsBase):
    id: str = field(compare=True)
    monday: bool = field(compare=False, repr=False)
    tuesday: bool = field(compare=False, repr=False)
    wednesday: bool = field(compare=False, repr=False)
    thursday: bool = field(compare=False, repr=False)
    friday: bool = field(compare=False, repr=False)
    saturday: bool = field(compare=False, repr=False)
    sunday: bool = field(compare=False, repr=False)
    start_date: Date = field(compare=False, repr=False)
    end_date: Date = field(compare=False, repr=False)
    desc: str = field(default="", compare=False, repr=False)

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
            monday INTEGER NOT NULL CHECK (monday IN (0, 1))
            tuesday INTEGER NOT NULL CHECK (monday IN (0, 1))
            wednesday INTEGER NOT NULL CHECK (monday IN (0, 1))
            thursday INTEGER NOT NULL CHECK (monday IN (0, 1))
            friday INTEGER NOT NULL CHECK (monday IN (0, 1))
            saturday INTEGER NOT NULL CHECK (monday IN (0, 1))
            sunday INTEGER NOT NULL CHECK (monday IN (0, 1))
            start_date TEXT NOT NULL CHECK (date LIKE '____-__-__'),
            end_date TEXT NOT NULL CHECK (date LIKE '____-__-__'),
            desc TEXT NOT NULL DEFAULT ''
        ) STRICT;"""

    @staticmethod
    def sql_placeholder() -> LiteralString:
        return "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"

    @staticmethod
    def sql_where_clause() -> LiteralString:
        return "calendar_id = ?"

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
