import sqlite3
import unittest
from pathlib import Path
from typing import cast

from impuls.db import DBConnection, EmptyQueryResult

FIXTURES = Path(__file__).parent / "fixtures"


class TestDBConnection(unittest.TestCase):
    def test_context_manager(self) -> None:
        with DBConnection(path=":memory:") as db:
            self.assertEqual(db.raw_execute("SELECT 0;").one(), (0,))

        with self.assertRaises(sqlite3.ProgrammingError):
            db.raw_execute("SELECT 0;")

    def test_transactions(self) -> None:
        with DBConnection(path=":memory:") as db:
            db.raw_execute("CREATE TABLE foo (id INTEGER PRIMARY KEY, name TEXT);")

            # At the beginning: no transaction, no data
            self.assertFalse(db.in_transaction)
            self.assertEqual(len(db.raw_execute("SELECT * FROM foo;").all()), 0)

            # Open transaction and add some rows
            db.begin()
            self.assertTrue(db.in_transaction)
            db.raw_execute_many("INSERT INTO foo VALUES (?, ?);", [(1, "one"), (2, "two")])
            db.commit()

            # Ensure transaction was successful
            self.assertFalse(db.in_transaction)
            self.assertListEqual(
                db.raw_execute("SELECT * FROM foo;").all(), [(1, "one"), (2, "two")]
            )

            # Open another transaction, but roll it back
            db.begin()
            self.assertTrue(db.in_transaction)
            db.raw_execute("INSERT INTO foo VALUES (?, ?);", (3, "three"))
            db.rollback()

            # Ensure transaction was rolled back
            self.assertFalse(db.in_transaction)
            self.assertListEqual(
                db.raw_execute("SELECT * FROM foo;").all(), [(1, "one"), (2, "two")]
            )

    def test_transaction_context_manager(self) -> None:
        with DBConnection(path=":memory:") as db:
            db.raw_execute("CREATE TABLE foo (id INTEGER PRIMARY KEY, name TEXT);")

            # At the beginning: no transaction, no data
            self.assertFalse(db.in_transaction)
            self.assertEqual(len(db.raw_execute("SELECT * FROM foo;").all()), 0)

            # Open transaction and add some rows
            with db.transaction():
                self.assertTrue(db.in_transaction)
                db.raw_execute_many("INSERT INTO foo VALUES (?, ?);", [(1, "one"), (2, "two")])

            # Ensure transaction was successful
            self.assertFalse(db.in_transaction)
            self.assertListEqual(
                db.raw_execute("SELECT * FROM foo;").all(), [(1, "one"), (2, "two")]
            )

            # Open another transaction, but roll it back
            try:
                with db.transaction():
                    self.assertTrue(db.in_transaction)
                    db.raw_execute("INSERT INTO foo VALUES (?, ?);", (3, "three"))
                    raise ValueError
            except ValueError:
                pass

            # Ensure transaction was rolled back
            self.assertFalse(db.in_transaction)
            self.assertListEqual(
                db.raw_execute("SELECT * FROM foo;").all(), [(1, "one"), (2, "two")]
            )

    def test_additional_functions(self) -> None:
        with DBConnection(path=":memory:") as db:
            self.assertEqual(
                db.raw_execute("SELECT unicode_lower('zaŻółĆ gĘŚlą JAźń');").one_must(""),
                ("zażółć gęślą jaźń",),
            )

            self.assertEqual(
                db.raw_execute("SELECT unicode_upper('zaŻółĆ gĘŚlą JAźń');").one_must(""),
                ("ZAŻÓŁĆ GĘŚLĄ JAŹŃ",),
            )

            self.assertEqual(
                db.raw_execute("SELECT unicode_casefold('zaŻółĆ gĘŚlą JAźń');").one_must(""),
                ("zażółć gęślą jaźń",),
            )

            self.assertEqual(
                db.raw_execute("SELECT unicode_title('zaŻółĆ gĘŚlą JAźń');").one_must(""),
                ("Zażółć Gęślą Jaźń",),
            )


class TestDBConnectionWithModel(unittest.TestCase):
    def test_create_schema(self) -> None:
        with DBConnection.create_with_schema(path=":memory:"):
            pass


class TestUntypedQueryResult(unittest.TestCase):
    def setUp(self) -> None:
        self.db = DBConnection(":memory:")
        with sqlite3.Connection(FIXTURES / "wkd.db") as con:
            con.backup(self.db._con)

    def tearDown(self) -> None:
        self.db.close()

    def test_context_manager_closes(self) -> None:
        with self.db.raw_execute("SELECT 0;") as cur:
            pass

        with self.assertRaises(sqlite3.ProgrammingError):
            cur.one()

    def test_iter(self) -> None:
        with self.db.raw_execute("SELECT route_id FROM routes;") as cur:
            self.assertListEqual(list(cur), [("A1",), ("ZA1",), ("ZA12",)])

    def test_one(self) -> None:
        with self.db.raw_execute("SELECT agency_id, name FROM agencies;") as cur:
            self.assertEqual(cur.one(), ("0", "Warszawska Kolej Dojazdowa"))
            self.assertIsNone(cur.one())

    def test_one_must(self) -> None:
        with self.db.raw_execute("SELECT agency_id, name FROM agencies;") as cur:
            self.assertTupleEqual(cur.one_must("first"), ("0", "Warszawska Kolej Dojazdowa"))
            with self.assertRaisesRegex(EmptyQueryResult, r"^second$"):
                cur.one_must("second")

    def test_many(self) -> None:
        with self.db.raw_execute("SELECT route_id FROM routes;") as cur:
            all: list[str] = []
            while chunk := cur.many():
                all.extend(cast(str, i[0]) for i in chunk)

            self.assertListEqual(all, ["A1", "ZA1", "ZA12"])

    def test_all(self) -> None:
        with self.db.raw_execute("SELECT route_id FROM routes;") as cur:
            self.assertListEqual(cur.all(), [("A1",), ("ZA1",), ("ZA12",)])
