from typing import Protocol, Sequence, Type, TypeVar

from typing_extensions import LiteralString

from ...tools.types import Self, SQLNativeType

EntityT = TypeVar("EntityT", bound="Entity")
"""EntityT is a helper TypeVar bound to an :py:class:`Entity`."""


class Entity(Protocol):
    """Entity is a protocol for marshalling data between model entities and SQL and GTFS.
    Every entity defined in the model implements this protocol.
    """

    @staticmethod
    def sql_table_name() -> LiteralString:
        """sql_table_name returns the SQL table name which holds entities of this type"""
        ...

    @staticmethod
    def sql_create_table() -> LiteralString:
        """sql_create_table returns the SQL CREATE TABLE statement necessary to hold
        entities of this type."""
        ...

    @staticmethod
    def sql_columns() -> LiteralString:
        """sql_columns returns a "(col1, col2, col3)" string used in SQL queries
        for this type. The number of question marks must match the number of elements
        returned by :py:meth:`sql_marshall`.
        """
        ...

    @staticmethod
    def sql_placeholder() -> LiteralString:
        """sql_placeholder returns a "(?, ?, ?, ?, ...)" string used in SQL queries
        for this type. The number of question marks must match the number of elements
        returned by :py:meth:`sql_marshall`."""
        ...

    @staticmethod
    def sql_where_clause() -> LiteralString:
        """sql_where_clause returns a "COLUMN_NAME = ? AND ..." string used in SQL queries
        to uniquely identify entities of this type."""
        ...

    @staticmethod
    def sql_set_clause() -> LiteralString:
        """sql_set_clause returns a "COLUMN_NAME = ?, OTHER_COLUMN = ?, ..."
        string used in UPDATE statements"""
        ...

    def sql_marshall(self) -> tuple[SQLNativeType, ...]:
        """sql_marshall converts an entity into its SQL representation."""
        ...

    def sql_primary_key(self) -> tuple[SQLNativeType, ...]:
        """sql_primary_key converts the primary key of an entity into its SQL representation.
        The returned tuple should have the same number of elements as :py:meth:`sql_where_clause`
        has parameters."""
        ...

    @classmethod
    def sql_unmarshall(cls: Type[Self], row: Sequence[SQLNativeType]) -> Self:
        """sql_unmarshall creates an entity from its SQL representation."""
        ...
