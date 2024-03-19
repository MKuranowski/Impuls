import re
import unittest
from abc import ABC, abstractmethod
from typing import Generic, Type

from impuls.model import EntityT

TABLE_NAME_REGEX = re.compile(r"^[a-z][a-z_]*[a-z]$")


class AbstractTestEntity:
    # NOTE: Nested classes are necessary to prevent abstract test cases
    #       from being discovered and run.
    #       See https://stackoverflow.com/a/50176291.

    class Template(ABC, unittest.TestCase, Generic[EntityT]):
        @abstractmethod
        def get_entity(self) -> EntityT:
            raise NotImplementedError

        @abstractmethod
        def get_type(self) -> Type[EntityT]:
            raise NotImplementedError

        def test_sql_table_name(self) -> None:
            self.assertRegex(self.get_type().sql_table_name(), TABLE_NAME_REGEX)

        def test_sql_columns(self) -> None:
            self.assertRegex(self.get_type().sql_columns(), r"^\((?:[a-z_]+, )*[a-z_]+\)")
            self.assertEqual(
                len(self.get_entity().sql_marshall()),
                self.get_type().sql_columns().count(",") + 1,
            )

        def test_sql_placeholder(self) -> None:
            self.assertRegex(self.get_type().sql_placeholder(), r"^\((?:\?, )*\?\)$")
            self.assertEqual(
                len(self.get_entity().sql_marshall()),
                self.get_type().sql_placeholder().count("?"),
            )

        def test_sql_where_clause(self) -> None:
            self.assertRegex(
                self.get_type().sql_where_clause(),
                r"^[a-z_]+ = \?(?: AND [a-z_]+ = \?)*$",
            )
            self.assertEqual(
                len(self.get_entity().sql_primary_key()),
                self.get_type().sql_where_clause().count("?"),
            )

        def test_sql_set_clause(self) -> None:
            self.assertRegex(
                self.get_type().sql_set_clause(), r"^[a-z_]+ = \?(?:, [a-z_]+ = \?)*$"
            )
            self.assertEqual(
                len(self.get_entity().sql_marshall()),
                self.get_type().sql_set_clause().count("?"),
            )

        @abstractmethod
        def test_sql_marshall(self) -> None:
            raise NotImplementedError

        @abstractmethod
        def test_sql_primary_key(self) -> None:
            raise NotImplementedError

        @abstractmethod
        def test_sql_unmarshall(self) -> None:
            raise NotImplementedError
