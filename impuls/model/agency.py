from dataclasses import dataclass, field
from typing import Sequence
from typing import Type as TypeOf
from typing import final

from typing_extensions import LiteralString

from ..tools.types import Self, SQLNativeType
from .meta.entity import Entity
from .meta.sql_builder import DataclassSQLBuilder


@final
@dataclass
class Agency(Entity):
    id: str
    name: str
    url: str = field(repr=False)
    timezone: str = field(repr=False)
    lang: str = field(default="", repr=False)
    phone: str = field(default="", repr=False)
    fare_url: str = field(default="", repr=False)

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
    def sql_columns() -> LiteralString:
        return "(agency_id, name, url, timezone, lang, phone, fare_url)"

    @staticmethod
    def sql_placeholder() -> LiteralString:
        return "(?, ?, ?, ?, ?, ?, ?)"

    @staticmethod
    def sql_where_clause() -> LiteralString:
        return "agency_id = ?"

    @staticmethod
    def sql_set_clause() -> LiteralString:
        return "agency_id = ?, name = ?, url = ?, timezone = ?, lang = ?, phone = ?, fare_url = ?"

    def sql_marshall(self) -> tuple[SQLNativeType, ...]:
        return (self.id, self.name, self.url, self.timezone, self.lang, self.phone, self.fare_url)

    def sql_primary_key(self) -> tuple[SQLNativeType, ...]:
        return (self.id,)

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
