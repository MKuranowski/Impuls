# © Copyright 2022-2024 Mikołaj Kuranowski
# SPDX-License-Identifier: GPL-3.0-or-later

import os
import re
import sqlite3
from contextlib import contextmanager
from typing import Any, Generator, Generic, Iterable, Optional, Sequence, Type, cast

from .model import ALL_MODEL_ENTITIES, Entity, EntityT
from .tools.types import Self, SQLNativeType, StrPath

__all__ = ["EmptyQueryResult", "UntypedQueryResult", "TypedQueryResult", "DBConnection"]


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
    this can be automatically done using ``with`` statements.
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

    @property
    def rowcount(self) -> int:
        """Read-only number of rows modified by INSERT, UPDATE or DELETE statement."""
        return self._cur.rowcount

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
        """Returns an arbitrary number of rows from the query result,
        selected for optimum performance.
        If the returned list has no elements - there are no more rows in the result set."""
        return self._cur.fetchmany()

    def all(self) -> list[SQLRow]:
        """Returns all remaining rows of the query result."""
        return self._cur.fetchall()

    def close(self) -> None:
        """Closes the resources used to access database results."""
        self._cur.close()


class TypedQueryResult(Generic[EntityT]):
    """TypedQueryResult is an object returned by SQL queries,
    which automatically unmarshalls objects of the Impuls data model.

    The interface of this object is otherwise similar to that of ``UntypedDataResult``:

    Apart from the .one()/.many()/.all() methods, TypedQueryResult support iteration.

    TypedQueryResult should be closed after usage -
    this can be automatically done using ``with`` statements.
    """

    def __init__(self, db_cursor: sqlite3.Cursor, typ: Type[EntityT]) -> None:
        self._cur: sqlite3.Cursor = db_cursor
        self._typ: Type[EntityT] = typ

    def __enter__(self: Self) -> Self:
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()

    def __iter__(self: Self) -> Self:
        return self

    def __next__(self) -> EntityT:
        return self._typ.sql_unmarshall(self._cur.__next__())

    @property
    def rowcount(self) -> int:
        """Read-only number of rows modified by INSERT, UPDATE or DELETE statement."""
        return self._cur.rowcount

    def one(self) -> Optional[EntityT]:
        """Returns the next row of the query result, or None if there are no more rows."""
        row = self._cur.fetchone()
        return self._typ.sql_unmarshall(row) if row else None

    def one_must(self, context: str) -> EntityT:
        """Returns the next row of the query result, or raises EmptyQueryResult with
        the provided context if there are no more rows."""
        r = self.one()
        if r is None:
            raise EmptyQueryResult(context)
        return r

    def many(self) -> list[EntityT]:
        """Returns an arbitrary number of rows from the query result,
        selected for optimum performance.
        If the returned list has no elements - there are no more rows in the result set."""
        return [self._typ.sql_unmarshall(i) for i in self._cur.fetchmany()]

    def all(self) -> list[EntityT]:
        """Returns all remaining rows of the query result."""
        return [self._typ.sql_unmarshall(i) for i in self._cur.fetchall()]

    def close(self) -> None:
        """Closes the resources used to access database results."""
        self._cur.close()


class DBConnection:
    """DBConnection represents a connection with an Impuls database.

    This is a thin wrapper around sqlite3.Connection that uses ImpulsBase interface
    to provide a dumb ORM engine.

    **Transactions**

    The database is run in an auto-commit mode - the user is fully responsible for
    managing transactions: unless .begin() is used,
    statements implicitly begin and commit a transaction.

    **ORM substitutions**

    Typed queries work by substituting 3 keywords in the passed SQL:

    * ``:table`` - replaced with the table name
    * ``:cols`` - replaced with comma-separated column names, in brackets
    * ``:vals`` - replaced with question marks (corresponding to table columns), in brackets
    * ``:set`` - replaced with "column_name=?, ...", without brackets
    * ``:where`` - replaced with "primary_key_column=? AND ...", without brackets.

    The substitutions maybe better explained by an example - to persist a CalendarException,
    it's enough to write the following query: ``INSERT INTO :table VALUES :vals;``.

    Such query will be automatically expanded to the following:
    ``INSERT INTO calendar_exceptions VALUES (?, ?, ?);``

    Similarly ``UPDATE :table SET :set WHERE :where;`` on CalendarException turns into
    ``UPDATE calendar_exceptions SET calendar_id = ?, date = ?, exception_type = ?
    WHERE calendar_id = ? AND date = ?;``

    .. warning::
        This class assumes that LiteralStrings returned by the entities' sql_* methods
        are safe to directly use in sql statements. It is the programmer's responsibility
        to ensure so.

    **Closing the DB**

    DBConnection's close() method releases resources held by the DBConnection.
    Any unclosed transactions are **not** closed.

    DBConnection can be used in a ``with`` statement - and such connection
    will be automatically closed upon exit from the with block.
    (Note that this behavior is different to sqlite3.Connection)

    **New SQL functions**

    For convenience several additional SQL function are provided,
    apart from those described at https://www.sqlite.org/lang_corefunc.html:

    * ``unicode_lower`` - equivalent to Python's str.lower
    * ``unicode_upper`` - equivalent to Python's str.upper
    * ``unicode_casefold`` - equivalent to Python's str.casefold
    * ``unicode_title`` - equivalent to Python's str.title
    * ``re_sub`` - equivalent to Python's re.sub
    """

    def __init__(self, path: StrPath) -> None:
        self._path = path
        self._con: sqlite3.Connection = sqlite3.connect(path)
        self._after_open()

    def _after_open(self) -> None:
        self._con.isolation_level = None
        self._con.execute("PRAGMA foreign_keys=1")
        self._con.execute("PRAGMA locking_mode=EXCLUSIVE")
        self._con.create_function("unicode_lower", 1, str.lower, deterministic=True)
        self._con.create_function("unicode_upper", 1, str.upper, deterministic=True)
        self._con.create_function("unicode_casefold", 1, str.casefold, deterministic=True)
        self._con.create_function("unicode_title", 1, str.title, deterministic=True)
        self._con.create_function("re_sub", 3, re.sub, deterministic=True)

    @classmethod
    def create_with_schema(cls: Type[Self], path: StrPath) -> Self:
        """Opens a new DB connection and executes DDL statements
        to prepare the database to hold Impuls model data."""
        statements: list[str] = [typ.sql_create_table() for typ in ALL_MODEL_ENTITIES]

        conn = cls(path)
        for statement in statements:
            conn._con.executescript(statement)
        return conn

    @classmethod
    def cloned(cls: Type[Self], from_: StrPath, in_: StrPath) -> Self:
        """Creates a new database inside ``in_`` with the contents of ``from_``.
        Returns a DBConnection to the new database.
        """
        self = cls(in_)
        with sqlite3.connect(from_) as source:
            source.backup(target=self._con)
        return self

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
        """Abstracts transactions in a ``with`` block.::

            with database.transaction():
                do_something_on(database)

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
        self,
        sql: str,
        parameters: Sequence[SQLNativeType] = (),
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

        Logically equivalent to::

            for parameter in parameters:
                raw_execute(sql, parameter)

        Except that results are collected into a single Cursor -
        which means SELECT queries can't be used with this function.
        """
        return UntypedQueryResult(self._con.executemany(sql, parameters))

    # Typed SQL handling:
    # Done by performing substitutions in the passed SQL statement:
    # ":table" → "table_name"
    # ":cols" → "(col1, col2, col3)"
    # ":vals" → "(?, ?, ?, ?, ...)"
    # ":set" → "col1=?, col2=?, col3=?, ..."
    # ":where" → "pk_col1=? AND pk_col2=? AND ..."

    @staticmethod
    def _sql_substitute_typed(sql: str, typ: Type[Entity]) -> str:
        return (
            sql.replace(":table", typ.sql_table_name())
            .replace(":cols", typ.sql_columns())
            .replace(":vals", typ.sql_placeholder())
            .replace(":set", typ.sql_set_clause())
            .replace(":where", typ.sql_where_clause())
        )

    def typed_in_execute(self, sql: str, parameters: Entity) -> UntypedQueryResult:
        """Executes a "typed" SQL query - ORM substitutions are made to the query.

        The ``parameters`` object is automatically converted to format accepted by the
        sqlite3 module. Results are passed unchanged.
        """
        return UntypedQueryResult(
            self._con.execute(
                self._sql_substitute_typed(sql, type(parameters)),
                parameters.sql_marshall(),
            )
        )

    def typed_in_execute_many(
        self, sql: str, typ: Type[EntityT], parameters: Iterable[EntityT]
    ) -> UntypedQueryResult:
        """Executes a "typed" SQL query - ORM substitutions are made to the query.

        The ``parameters`` objects are automatically converted to format accepted by the
        sqlite3 module. Results are passed unchanged.

        Logically equivalent to::

            for parameter in parameters:
                raw_execute(sql, parameter)

        Except that results are collected into a single Cursor -
        which means SELECT queries can't be used with this function.
        """
        return UntypedQueryResult(
            self._con.executemany(
                self._sql_substitute_typed(sql, typ),
                (i.sql_marshall() for i in parameters),
            )
        )

    def typed_out_execute(
        self, sql: str, typ: Type[EntityT], parameters: Sequence[SQLNativeType] = ()
    ) -> TypedQueryResult[EntityT]:
        """Executes a "typed" SQL query - ORM substitutions are made to the query.

        The ``parameters`` are passed unchanged to the sqlite3 module.
        Results are automatically converted to instances of ``typ`` ImpulsBase objects.
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

    def retrieve(self, typ: Type[EntityT], *pk: SQLNativeType) -> Optional[EntityT]:
        """Retrieves an object of type ``typ`` with given primary key (usually its ID)
        from the database.

        Returns ``None`` if no such object is found.
        """
        return self.typed_out_execute("SELECT * FROM :table WHERE :where", typ, pk).one()

    def retrieve_must(self, typ: Type[EntityT], *pk: SQLNativeType) -> EntityT:
        """Retrieves an object of type ``typ`` with given primary key (usually its ID)
        from the database.

        Raises EmptyQueryResult if no such object is found.
        """
        return self.typed_out_execute("SELECT * FROM :table WHERE :where", typ, pk).one_must(
            f"No {typ.__name__} with the following primary key: {pk}"
        )

    def retrieve_all(self, typ: Type[EntityT]) -> TypedQueryResult[EntityT]:
        """Retrieves all objects of specific type from the database"""
        return self.typed_out_execute("SELECT * FROM :table", typ)

    def create(self, entity: Entity) -> None:
        """Creates a new entity in the database"""
        self.typed_in_execute("INSERT INTO :table :cols VALUES :vals", entity)

    def create_many(self, typ: Type[EntityT], entities: Iterable[EntityT]) -> None:
        """Creates multiple entries in the database"""
        self.typed_in_execute_many("INSERT INTO :table :cols VALUES :vals", typ, entities)

    def update(self, entity: Entity) -> None:
        """Updates the attributes of an entity in the database"""
        typ = type(entity)
        self.raw_execute(
            self._sql_substitute_typed("UPDATE :table SET :set WHERE :where", typ),
            (*entity.sql_marshall(), *entity.sql_primary_key()),
        )

    def update_many(self, typ: Type[EntityT], entities: Iterable[EntityT]) -> None:
        """Updates the attributes of multiple entries in the database"""
        self.raw_execute_many(
            self._sql_substitute_typed("UPDATE :table SET :set WHERE :where", typ),
            ((*i.sql_marshall(), *i.sql_primary_key()) for i in entities),
        )

    def count(self, typ: Type[EntityT]) -> int:
        """Returns the amount of instances of the provided type"""
        return cast(
            int,
            self.raw_execute(
                self._sql_substitute_typed("SELECT COUNT(*) FROM :table", typ)
            ).one_must("SELECT COUNT(*) must return one row")[0],
        )

    @contextmanager
    def released(self) -> Generator[str, None, None]:
        """Returns the path of the database, temporarily closing the connection.

        Any operations on the database within the body of the contextmanager are not
        permitted. This is intended for interfacing with other programs which expect
        a path to a SQLite database.
        """
        assert self._path != "", "can't release a private database"
        assert self._path != ":memory:", "can't release a in-memory database"

        self._con.close()
        try:
            yield os.fsdecode(self._path)
        finally:
            self._con = sqlite3.Connection(self._path)
            self._after_open()
