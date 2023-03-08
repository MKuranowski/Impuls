from dataclasses import dataclass, field
from typing import Mapping, Sequence
from typing import Type as TypeOf
from typing import final

from typing_extensions import LiteralString

from ..tools.types import Self, SQLNativeType
from .meta import DataclassGTFSBuilder, DataclassSQLBuilder, ImpulsBase


@final
@dataclass(unsafe_hash=True)
class FeedInfo(ImpulsBase):
    publisher_name: str = field(compare=False)
    publisher_url: str = field(compare=False, repr=False)
    lang: str = field(compare=False, repr=False)
    version: str = field(default="", compare=True)
    contact_email: str = field(default="", compare=False, repr=False)
    contact_url: str = field(default="", compare=False, repr=False)

    id: str = field(default="0", repr=False)
    """id of the FeedInfo must be always \"0\", as there can only be
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
        }

    @classmethod
    def gtfs_unmarshall(cls: TypeOf[Self], row: Mapping[str, str]) -> Self:
        return cls(
            **DataclassGTFSBuilder(row)
            .field("publisher_name", "feed_publisher_name")
            .field("publisher_url", "feed_publisher_url")
            .field("lang", "feed_lang")
            .field("version", "feed_version")
            .field("contact_email", "feed_contact_email")
            .field("contact_url", "feed_contact_url")
            .kwargs()
        )

    @staticmethod
    def sql_table_name() -> LiteralString:
        return "feed_info"

    @staticmethod
    def sql_create_table() -> LiteralString:
        return """CREATE TABLE feed_info (
            feed_info_id TEXT PRIMARY KEY CHECK (feed_info_id = '0'),
            publisher_name TEXT NOT NULL,
            publisher_url TEXT NOT NULL,
            lang TEXT NOT NULL,
            version TEXT NOT NULL DEFAULT '',
            contact_email TEXT NOT NULL DEFAULT '',
            contact_url TEXT NOT NULL DEFAULT ''
        ) STRICT;"""

    @staticmethod
    def sql_placeholder() -> LiteralString:
        return "(?, ?, ?, ?, ?, ?, ?)"

    @staticmethod
    def sql_where_clause() -> LiteralString:
        return "feed_info_id = '0'"

    def sql_marshall(self) -> tuple[SQLNativeType, ...]:
        return (
            self.id,
            self.publisher_name,
            self.publisher_url,
            self.lang,
            self.version,
            self.contact_email,
            self.contact_url,
        )

    @classmethod
    def sql_unmarshall(cls: TypeOf[Self], row: Sequence[SQLNativeType]) -> Self:
        return cls(
            **DataclassSQLBuilder(row)
            .field("id", str)
            .field("publisher_name", str)
            .field("publisher_url", str)
            .field("lang", str)
            .field("version", str)
            .field("contact_email", str)
            .field("contact_url", str)
            .kwargs()
        )
