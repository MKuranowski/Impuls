import unittest
from enum import IntEnum
from typing import Union

from impuls.model.meta.sql_builder import DataclassSQLBuilder


class Direction(IntEnum):
    UP = 0
    RIGHT = 1
    DOWN = 2
    LEFT = 3


class TestDataclassSQLBuilder(unittest.TestCase):
    def test(self) -> None:
        b = DataclassSQLBuilder((1, "pi", 3.14))
        b.field("id", int)
        b.field("name", str)
        b.field("value", float)

        self.assertDictEqual(
            b.kwargs(),
            {
                "id": 1,
                "name": "pi",
                "value": 3.14,
            },
        )

    def test_too_many_fields(self) -> None:
        with self.assertRaisesRegex(RuntimeError, r"Too many fields"):
            DataclassSQLBuilder((1,)).field("id", int).field("name", str).kwargs()

    def test_too_few_fields(self) -> None:
        with self.assertRaisesRegex(RuntimeError, r"Too few fields"):
            DataclassSQLBuilder((1, "Foo")).field("id", int).kwargs()

    def test_type_check(self) -> None:
        with self.assertRaises(TypeError):
            DataclassSQLBuilder(("1",)).field("id", int).kwargs()

    def test_type_checks_union(self) -> None:
        self.assertDictEqual(
            DataclassSQLBuilder(("1",)).field("id", Union[str, int]).kwargs(),
            {"id": "1"},
        )

        self.assertDictEqual(
            DataclassSQLBuilder((1,)).field("id", Union[str, int]).kwargs(),
            {"id": 1},
        )

        with self.assertRaises(TypeError):
            DataclassSQLBuilder((None,)).field("id", Union[str, int]).kwargs()

    def test_converter(self) -> None:
        self.assertDictEqual(
            DataclassSQLBuilder((2,)).field("dir", int, Direction).kwargs(),
            {"dir": Direction.DOWN},
        )

        with self.assertRaises(ValueError):
            DataclassSQLBuilder((5,)).field("dir", int, Direction).kwargs()

    def test_bool_converter(self) -> None:
        b = DataclassSQLBuilder((0, 1))
        b.field("zero", bool)
        b.field("one", bool)
        d = b.kwargs()

        self.assertIs(d["zero"], False)
        self.assertIs(d["one"], True)

    def test_nullable(self) -> None:
        b = DataclassSQLBuilder((1, None))
        b.field("concrete", int, nullable=True)
        b.field("null", int, nullable=True)
        d = b.kwargs()

        self.assertEqual(d["concrete"], 1)
        self.assertIsNone(d["null"])

    def test_nullable_bool(self) -> None:
        b = DataclassSQLBuilder((0, 1, None))
        b.field("zero", bool, nullable=True)
        b.field("one", bool, nullable=True)
        b.field("null", bool, nullable=True)
        d = b.kwargs()

        self.assertIs(d["zero"], False)
        self.assertIs(d["one"], True)
        self.assertIsNone(d["null"])

    def test_nullable_converter(self) -> None:
        b = DataclassSQLBuilder((1, None))
        b.field("right", int, converter=Direction, nullable=True)
        b.field("null", int, converter=Direction, nullable=True)
        d = b.kwargs()

        self.assertIs(d["right"], Direction.RIGHT)
        self.assertIsNone(d["null"])

        with self.assertRaises(ValueError):
            DataclassSQLBuilder((5,)).field(
                "dir", int, converter=Direction, nullable=True
            ).kwargs()
