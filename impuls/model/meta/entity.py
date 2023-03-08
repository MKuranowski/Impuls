from typing import Mapping, Protocol, Sequence, Type, TypeVar

from typing_extensions import LiteralString

from ...tools.types import Self, SQLNativeType

EntityT = TypeVar("EntityT", bound="Entity")


class Entity(Protocol):
    """Entity is a protocol for marshalling data between model entities and SQL and GTFS.
    Every entity defined in the model implements this protocol.
    """

    @staticmethod
    def gtfs_table_name() -> LiteralString:
        """gtfs_table_name returns the table name (without .txt suffix) in GTFS,
        which holds entities of this type."""
        ...

    def gtfs_marshall(self) -> dict[str, str]:
        """gtfs_marshall converts an entity into its GTFS representation."""
        ...

    @classmethod
    def gtfs_unmarshall(cls: Type[Self], row: Mapping[str, str]) -> Self:
        """gtfs_unmarshall creates an entity from its GTFS representation."""
        ...

    @staticmethod
    def sql_table_name() -> LiteralString:
        """sql_table_name returns the SQL table name which holds entities of this type"""
        ...

    @staticmethod
    def sql_create_table() -> LiteralString:
        """sql_create_table returns the SQL CREATE TABLE statement."""
        ...

    @staticmethod
    def sql_placeholder() -> LiteralString:
        """sql_placeholder returns a (?, ?, ?, ?, ...) string used in SQL queries
        for this type. The number of question marks must match the number of elements
        expected by sql_unmarshall and returned by sql_marshall."""
        ...

    @staticmethod
    def sql_where_clause() -> LiteralString:
        """sql_where_clause returns a (COLUMN_NAME = ? AND ...) string used in SQL queries
        to uniquely identify entities of this type."""
        ...

    def sql_marshall(self) -> tuple[SQLNativeType, ...]:
        """sql_marshall converts an entity into its SQL representation."""
        ...

    @classmethod
    def sql_unmarshall(cls: Type[Self], row: Sequence[SQLNativeType]) -> Self:
        """sql_unmarshall creates an entity from its SQL representation."""
        ...
