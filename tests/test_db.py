import sqlite3
import unittest

from impuls.db import DBConnection


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
