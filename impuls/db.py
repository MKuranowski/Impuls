from contextlib import contextmanager
import sqlite3
from itertools import repeat
from typing import Any, Generator, Generic, Iterable, Sequence, Type, TypeVar

from .model import ALL_MODEL_ENTITIES, ImpulsBase
from .tools.types import Self, SQLNativeType

__all__ = ["EmptyQueryResult", "UntypedQueryResult", "TypedQueryResult", "DBConnection"]


_IB = TypeVar("_IB", bound=ImpulsBase)
SQLRow = tuple[SQLNativeType, ...]


class EmptyQueryResult(ValueError):
    """EmptyQueryResult is an exception used when an SQL query returned an empty result,
    even tough the application expected at least one row."""

    pass


class UntypedQueryResult:
    """UntypedQueryResult is an object returned by SQL queries,
    which returns results of unknown types.

    Apart from the .one()/.many()/.all() methods,
    UntypedQueryResults support iteration.

    UntypedQueryResult should be closed after usage -
    this can be automatically done using `with` statements.
    """

    def __init__(self, db_cursor: sqlite3.Cursor) -> None:
        self._cur: sqlite3.Cursor = db_cursor

    def __enter__(self: Self) -> Self:
        return self

    def __exit__(self, *_: Any) -> None:
        self._cur.close()

    def __iter__(self: Self) -> Self:
        return self

    def __next__(self) -> SQLRow:
        return self._cur.__next__()

    def one(self) -> SQLRow | None:
        """Returns the next row of the query result, or None if there are no more rows."""
        return self._cur.fetchone()

    def one_must(self, context: str) -> SQLRow:
        """Returns the next row of the query result, or raises EmptyQueryResult with
        the provided context if there are no more rows."""
        r = self.one()
        if r is None:
            raise EmptyQueryResult(context)
        return r

    def many(self) -> list[SQLRow]:
        """Returns an arbitrary number (selected of optimum performance) of rows from the query result.
        If the returned list has no elements - there are no more rows in the result set."""
        return self._cur.fetchmany()

    def all(self) -> list[SQLRow]:
        """Returns all remaining rows of the query result."""
        return self._cur.fetchall()

    def close(self) -> None:
        """Closes the resources used to access database results."""
        self._cur.close()


class TypedQueryResult(Generic[_IB]):
    """TypedQueryResult is an object returned by SQL queries,
    which automatically unmarshalls objects of the Impuls data model.

    The interface of this object is otherwise similar to that of `UntypedDataResult`:

    Apart from the .one()/.many()/.all() methods, TypedQueryResult support iteration.

    TypedQueryResult should be closed after usage -
    this can be automatically done using `with` statements.
    """

    def __init__(self, db_cursor: sqlite3.Cursor, typ: Type[_IB]) -> None:
        self._cur: sqlite3.Cursor = db_cursor
        self._typ: Type[_IB] = typ

    def __enter__(self: Self) -> Self:
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()

    def __iter__(self: Self) -> Self:
        return self

    def __next__(self) -> _IB:
        return self._typ._sql_unmarshall(self._cur.__next__())

    def one(self) -> _IB | None:
        """Returns the next row of the query result, or None if there are no more rows."""
        row = self._cur.fetchone()
        return self._typ._sql_unmarshall(row) if row else None

    def one_must(self, context: str) -> _IB:
        """Returns the next row of the query result, or raises EmptyQueryResult with
        the provided context if there are no more rows."""
        r = self.one()
        if r is None:
            raise EmptyQueryResult(context)
        return r

    def many(self) -> list[_IB]:
        """Returns an arbitrary number (selected of optimum performance) of rows from the query result.
        If the returned list has no elements - there are no more rows in the result set."""
        return [self._typ._sql_unmarshall(i) for i in self._cur.fetchmany()]

    def all(self) -> list[_IB]:
        """Returns all remaining rows of the query result."""
        return [self._typ._sql_unmarshall(i) for i in self._cur.fetchall()]

    def close(self) -> None:
        """Closes the resources used to access database results."""
        self._cur.close()


class DBConnection:
    """DBConnection represents a connection with an Impuls database.

    This is a thin wrapper around sqlite3.Connection that uses ImpulsBase interface
    to provide a dumb ORM engine.

    ### Transactions

    The database is run in an auto-commit mode - the user is fully responsible for
    managing transactions: unless .begin() is used,
    statements implicitly begin and commit a transaction.

    ### ORM substitutions

    Typed queries work by substituting 3 keywords in the passed SQL:
    - `:table` - replaced with the table name
    - `:cols` - replaced with the column names, in brackets
    - `:vals` - replaced with question marks (corresponding to table columns), in brackets

    The substitutions maybe better explained by an example - to persist a CalendarException,
    it's enough to write the following query:
    `INSERT INTO :table :cols VALUES :vals;`.

    Such query will be automatically expanded to the following:
    `INSERT INTO calendar_exceptions (calendar_id, date, exception_type) VALUES (?, ?, ?);`

    ### Closing the DB

    DBConnection's close() method releases resources held by the DBConnection.
    Any unclosed transactions are **not** closed.

    DBConnection can be used in a `with` statement - and such connection
    will be automatically closed upon exit from the with block.
    (Note that this behavior is different to sqlite3.Connection)
    """

    def __init__(self, path: str = ":memory:") -> None:
        self._con: sqlite3.Connection = sqlite3.connect(path)
        self._con.isolation_level = None

    @classmethod
    def create_with_schema(cls: Type[Self], path: str = ":memory:") -> Self:
        """Opens a new DB connection and executes DDL statements
        to prepare the database to hold Impuls model data."""
        # NOTE: We assume that impuls_base decorator has generated safe field descriptions

        # List of top-level statements (mostly CREATE TABLE)
        creates: list[str] = ["PRAGMA foreign_keys=1;", "PRAGMA locking_mode=EXCLUSIVE;"]

        # Create a table for every entity of the model
        for typ in ALL_MODEL_ENTITIES:
            # Collect all columns, indexes and the primary key of the table
            columns: list[str] = []
            columns_to_index: list[str] = []
            primary_key_columns: list[str] = []

            for c in typ._sql_fields.values():
                # Start with "column_name TYPE"
                column_def: list[str] = [c.column_name, c.sql_type]

                # Add foreign key constraint if necessary
                if c.foreign_key:
                    column_def.append(
                        f"REFERENCES {c.foreign_key} ON DELETE CASCADE ON UPDATE CASCADE"
                    )

                # Add NOT NULL constraint if necessary
                if c.not_null:
                    column_def.append("NOT NULL")

                # Remember which columns form the primary key
                if c.primary_key:
                    primary_key_columns.append(c.column_name)

                # Remember which columns should be indexed
                if c.indexed:
                    columns_to_index.append(c.column_name)

                # Save the column definition
                columns.append(" ".join(column_def))

            # Generate the primary key constraint
            columns.append(f"PRIMARY KEY ({', '.join(primary_key_columns)})")

            # Generate the CREATE TABLE statement
            col_sep = ",\n\t"
            creates.append(
                f"CREATE TABLE {typ._sql_table_name} (\n\t{col_sep.join(columns)}\n) STRICT;"
            )

            # Generate the CREATE INDEX statements
            for column_to_index in columns_to_index:
                creates.append(
                    f"CREATE INDEX idx_{typ._sql_table_name}_{column_to_index} ON "
                    f"{typ._sql_table_name} ({column_to_index});"
                )

        # Cast the script into a string and execute it
        script = "\n".join(creates)

        conn = cls(path)
        conn._con.executescript(script)
        return conn

    # Resource handling

    def __enter__(self: Self) -> Self:
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()

    def close(self) -> None:
        """Closes any handles used to communicate with the sqlite3 engine.
        Any open transactions are **not** implicitly committed.
        """
        self._con.close()

    # Transaction handling

    @property
    def in_transaction(self) -> bool:
        """Proxy to sqlite3.Connection.in_transaction - should be True
        if there's an ongoing transaction."""
        return self._con.in_transaction

    def begin(self) -> None:
        """Manually starts a transaction"""
        self._con.execute("BEGIN TRANSACTION;")

    def commit(self) -> None:
        """Commits an ongoing transaction, persisting any changes made to the DB"""
        self._con.commit()

    def rollback(self) -> None:
        """Rolls back an ongoing transaction, reverting any changes made to the DB"""
        self._con.rollback()

    @contextmanager
    def transaction(self: Self) -> Generator[Self, None, None]:
        """Abstracts transactions in a `with` block.

        >>> with database.transaction():  # doctest: +SKIP
        ...     do_something_on(database)

        A transaction is opened at the entry to the with block.
        If an exception is raised in the body, the changes are rolled back.
        Otherwise, the changes are automatically committed.
        """
        self.begin()
        try:
            yield self
        except Exception:
            self.rollback()
            raise
        else:
            self.commit()

    # Untyped SQL handling

    def raw_execute(
        self, sql: str, parameters: Sequence[SQLNativeType] = ()
    ) -> UntypedQueryResult:
        """Executes a "raw" SQL query - no ORM substitutions are made in the query.
        The parameters and results are passed unchanged to/from the sqlite3 module.
        """
        return UntypedQueryResult(self._con.execute(sql, parameters))

    def raw_execute_many(
        self, sql: str, parameters: Iterable[Sequence[SQLNativeType]]
    ) -> UntypedQueryResult:
        """Executes a "raw" SQL query multiple times -
        no ORM substitutions are made in the query.

        The parameters and results are passed unchanged to/from the sqlite3 module.

        Logically equivalent to
        >>> for parameter in parameters:  # doctest: +SKIP
        ...     raw_execute(sql, parameter)
        Except that results are collected into a single Cursor -
        which means SELECT queries can't be used with this function.
        """
        return UntypedQueryResult(self._con.executemany(sql, parameters))

    # Typed SQL handling:
    # Done by performing substitutions in the passed SQL statement:
    # ":table" → "table_name"
    # ":cols" → "(col1, col2, col3, ...)"
    # ":vals" → "(?, ?, ?, ?, ...)"

    @staticmethod
    def _sql_substitute_typed(sql: str, typ: Type[ImpulsBase]) -> str:
        cols = f'({", ".join(field.column_name for field in typ._sql_fields.values())})'
        vals = f'({", ".join(repeat("?", len(typ._sql_fields)))})'
        return (
            sql.replace(":table", typ._sql_table_name)
            .replace(":cols", cols)
            .replace(":vals", vals)
        )

    @staticmethod
    def _sql_pk_where_body(typ: Type[ImpulsBase]) -> str:
        return " AND ".join(
            f"{field.column_name} = ?" for field in typ._sql_primary_key_columns.values()
        )

    def typed_in_execute(self, sql: str, parameters: ImpulsBase) -> UntypedQueryResult:
        """Executes a "typed" SQL query - ORM substitutions are made to the query.

        The `parameters` object is automatically converted to format accepted by the
        sqlite3 module. Results are passed unchanged.
        """
        return UntypedQueryResult(
            self._con.execute(
                self._sql_substitute_typed(sql, type(parameters)),
                parameters._sql_marshall(),
            )
        )

    def typed_in_execute_many(
        self, sql: str, typ: Type[_IB], parameters: Iterable[_IB]
    ) -> UntypedQueryResult:
        """Executes a "typed" SQL query - ORM substitutions are made to the query.

        The `parameters` objects are automatically converted to format accepted by the
        sqlite3 module. Results are passed unchanged.

        Logically equivalent to
        >>> for parameter in parameters:  # doctest: +SKIP
        ...     raw_execute(sql, parameter)
        Except that results are collected into a single Cursor -
        which means SELECT queries can't be used with this function.
        """
        return UntypedQueryResult(
            self._con.executemany(
                self._sql_substitute_typed(sql, typ),
                (i._sql_marshall() for i in parameters),
            )
        )

    def typed_out_execute(
        self, sql: str, typ: Type[_IB], parameters: Sequence[SQLNativeType] = ()
    ) -> TypedQueryResult[_IB]:
        """Executes a "typed" SQL query - ORM substitutions are made to the query.

        The `parameters` are passed unchanged to the sqlite3 module.
        Results are automatically converted to instances of `typ` ImpulsBase objects.
        """
        return TypedQueryResult(
            self._con.execute(
                self._sql_substitute_typed(sql, typ),
                parameters,
            ),
            typ,
        )

    # NOTE: No typed_out_execute_many. The only way to retrieve data from the DB
    #       is through SELECT statements, which are not permitted in executemany.

    # Simple methods for working on the entities form the model

    def retrieve(self, typ: Type[_IB], *pk: SQLNativeType) -> _IB | None:
        """Retrieves an object of type `typ` with given primary key (usually its ID)
        from the database.

        Returns `None` if no such object is found.
        """
        return self.typed_out_execute(
            f"SELECT * FROM :table WHERE {self._sql_pk_where_body(typ)}",
            typ,
            pk,
        ).one()

    def retrieve_must(self, typ: Type[_IB], *pk: SQLNativeType) -> _IB:
        """Retrieves an object of type `typ` with given primary key (usually its ID)
        from the database.

        Raises EmptyQueryResult if no such object is found
        """
        return self.typed_out_execute(
            f"SELECT * FROM :table WHERE {self._sql_pk_where_body(typ)}",
            typ,
            pk,
        ).one_must(f"No {typ.__name__} with the following primary key: {pk}")

    def retrieve_all(self, typ: Type[_IB]) -> TypedQueryResult[_IB]:
        """Retrieves all objects of specific type from the database"""
        return self.typed_out_execute("SELECT * FROM :table", typ)

    def save(self, entity: ImpulsBase) -> None:
        """Ensures given entity is stored in the database by executing an
        `INSERT OR REPLACE` query."""
        self.typed_in_execute("INSERT OR REPLACE INTO :table VALUES :vals", entity)
