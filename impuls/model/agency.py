from dataclasses import dataclass, field
from typing import Mapping, Sequence
from typing import Type as TypeOf
from typing import final

from typing_extensions import LiteralString

from ..tools.types import Self, SQLNativeType
from .meta.entity import Entity
from .meta.gtfs_builder import DataclassGTFSBuilder
from .meta.sql_builder import DataclassSQLBuilder


@final
@dataclass(unsafe_hash=True)
class Agency(Entity):
    id: str = field(compare=True)
    name: str = field(compare=False)
    url: str = field(compare=False, repr=False)
    timezone: str = field(compare=False, repr=False)
    lang: str = field(default="", compare=False, repr=False)
    phone: str = field(default="", compare=False, repr=False)
    fare_url: str = field(default="", compare=False, repr=False)

    @staticmethod
    def gtfs_table_name() -> LiteralString:
        return "agency"

    def gtfs_marshall(self) -> dict[str, str]:
        return {
            "agency_id": self.id,
            "agency_name": self.name,
            "agency_url": self.url,
            "agency_timezone": self.timezone,
            "agency_lang": self.lang,
            "agency_phone": self.phone,
            "agency_fare_url": self.fare_url,
        }

    @classmethod
    def gtfs_unmarshall(cls: TypeOf[Self], row: Mapping[str, str]) -> Self:
        return cls(
            **DataclassGTFSBuilder(row)
            .field("id", "agency_id")
            .field("name", "agency_name")
            .field("url", "agency_url")
            .field("timezone", "agency_timezone")
            .field("lang", "agency_lang", fallback_value="")
            .field("phone", "agency_phone", fallback_value="")
            .field("fare_url", "agency_fare_url", fallback_value="")
            .kwargs()
        )

    @staticmethod
    def sql_table_name() -> LiteralString:
        return "agencies"

    @staticmethod
    def sql_create_table() -> LiteralString:
        return """CREATE TABLE agencies (
            agency_id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            url TEXT NOT NULL,
            timezone TEXT NOT NULL,
            lang TEXT NOT NULL DEFAULT '',
            phone TEXT NOT NULL DEFAULT '',
            fare_url TEXT NOT NULL DEFAULT ''
        ) STRICT;"""

    @staticmethod
    def sql_placeholder() -> LiteralString:
        return "(?, ?, ?, ?, ?, ?, ?)"

    @staticmethod
    def sql_where_clause() -> LiteralString:
        return "agency_id = ?"

    def sql_marshall(self) -> tuple[SQLNativeType, ...]:
        return (self.id, self.name, self.url, self.timezone, self.lang, self.phone, self.fare_url)

    @classmethod
    def sql_unmarshall(cls: TypeOf[Self], row: Sequence[SQLNativeType]) -> Self:
        return cls(
            **DataclassSQLBuilder(row)
            .field("id", str)
            .field("name", str)
            .field("url", str)
            .field("timezone", str)
            .field("lang", str)
            .field("phone", str)
            .field("fare_url", str)
            .kwargs()
        )
