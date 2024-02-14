from dataclasses import dataclass, field
from typing import Mapping, Optional, Sequence
from typing import Type as TypeOf
from typing import final

from typing_extensions import LiteralString

from ..tools.types import Self, SQLNativeType
from .meta.entity import Entity
from .meta.gtfs_builder import DataclassGTFSBuilder
from .meta.sql_builder import DataclassSQLBuilder
from .meta.utility_types import Date


@final
@dataclass
class FeedInfo(Entity):
    publisher_name: str
    publisher_url: str = field(repr=False)
    lang: str = field()
    version: str = field(default="")
    contact_email: str = field(default="", repr=False)
    contact_url: str = field(default="", repr=False)
    start_date: Optional[Date] = field(default=None)
    end_date: Optional[Date] = field(default=None)

    id: int = field(default=0, repr=False)
    """id of the FeedInfo must be always 0, as there can only be
    entry in the feed_info table."""

    @staticmethod
    def gtfs_table_name() -> LiteralString:
        return "feed_info"

    def gtfs_marshall(self) -> dict[str, str]:
        return {
            "feed_publisher_name": self.publisher_name,
            "feed_publisher_url": self.publisher_url,
            "feed_lang": self.lang,
            "feed_version": self.version,
            "feed_contact_email": self.contact_email,
            "feed_contact_url": self.contact_url,
            "feed_start_date": self.start_date.strftime("%Y%m%d") if self.start_date else "",
            "feed_end_date": self.end_date.strftime("%Y%m%d") if self.end_date else "",
        }

    @classmethod
    def gtfs_unmarshall(cls: TypeOf[Self], row: Mapping[str, str]) -> Self:
        return cls(
            **DataclassGTFSBuilder(row)
            .field("publisher_name", "feed_publisher_name")
            .field("publisher_url", "feed_publisher_url")
            .field("lang", "feed_lang")
            .field("version", "feed_version", fallback_value="")
            .field("contact_email", "feed_contact_email", fallback_value="")
            .field("contact_url", "feed_contact_url", fallback_value="")
            .field(
                "start_date",
                "feed_start_date",
                converter=lambda s: Date.from_ymd_str(s) if s else None,
                fallback_value=None,
            )
            .field(
                "end_date",
                "feed_end_date",
                converter=lambda s: Date.from_ymd_str(s) if s else None,
                fallback_value=None,
            )
            .kwargs()
        )

    @staticmethod
    def sql_table_name() -> LiteralString:
        return "feed_info"

    @staticmethod
    def sql_create_table() -> LiteralString:
        return """CREATE TABLE feed_info (
            feed_info_id INTEGER PRIMARY KEY CHECK (feed_info_id = '0'),
            publisher_name TEXT NOT NULL,
            publisher_url TEXT NOT NULL,
            lang TEXT NOT NULL,
            version TEXT NOT NULL DEFAULT '',
            contact_email TEXT NOT NULL DEFAULT '',
            contact_url TEXT NOT NULL DEFAULT '',
            start_date TEXT DEFAULT NULL CHECK (start_date LIKE '____-__-__'),
            end_date TEXT DEFAULT NULL CHECK (end_date LIKE '____-__-__')
        ) STRICT;"""

    @staticmethod
    def sql_placeholder() -> LiteralString:
        return "(?, ?, ?, ?, ?, ?, ?, ?, ?)"

    @staticmethod
    def sql_where_clause() -> LiteralString:
        return "feed_info_id = ?"

    @staticmethod
    def sql_set_clause() -> LiteralString:
        return (
            "feed_info_id = ?, publisher_name = ?, publisher_url = ?, lang = ?, "
            "version = ?, contact_email = ?, contact_url = ?, start_date = ?, end_date = ?"
        )

    def sql_marshall(self) -> tuple[SQLNativeType, ...]:
        return (
            self.id,
            self.publisher_name,
            self.publisher_url,
            self.lang,
            self.version,
            self.contact_email,
            self.contact_url,
            str(self.start_date) if self.start_date else None,
            str(self.end_date) if self.end_date else None,
        )

    def sql_primary_key(self) -> tuple[SQLNativeType, ...]:
        return (self.id,)

    @classmethod
    def sql_unmarshall(cls: TypeOf[Self], row: Sequence[SQLNativeType]) -> Self:
        return cls(
            **DataclassSQLBuilder(row)
            .field("id", int)
            .field("publisher_name", str)
            .field("publisher_url", str)
            .field("lang", str)
            .field("version", str)
            .field("contact_email", str)
            .field("contact_url", str)
            .field("start_date", str, Date.from_ymd_str, nullable=True)
            .field("end_date", str, Date.from_ymd_str, nullable=True)
            .kwargs()
        )
