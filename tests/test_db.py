import sqlite3
import unittest
from pathlib import Path
from typing import cast

from impuls.db import DBConnection, EmptyQueryResult
from impuls.model import Agency, Route
from impuls.tools.testing_mocks import MockFile

FIXTURES = Path(__file__).parent / "fixtures"


class TestWithoutModel(unittest.TestCase):
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

    def test_released(self) -> None:
        with MockFile() as f, DBConnection(f) as db:
            with db:
                db.raw_execute("CREATE TABLE numbers (number INTEGER PRIMARY KEY)")
                db.raw_execute("INSERT INTO numbers VALUES (42)")
                db.raw_execute("INSERT INTO numbers VALUES (2137)")

            with db.released() as db_path:
                self.assertEqual(db_path, str(f))

                with DBConnection(db_path) as another_db_handle:
                    self.assertSetEqual(
                        set(another_db_handle.raw_execute("SELECT * FROM numbers")),
                        {(42,), (2137,)},
                    )


class TestCreatesSchema(unittest.TestCase):
    def test(self) -> None:
        with DBConnection.create_with_schema(path=":memory:"):
            pass


class TestWithModel(unittest.TestCase):
    def setUp(self) -> None:
        self.db = DBConnection(":memory:")
        with sqlite3.Connection(FIXTURES / "wkd.db") as con:
            con.backup(self.db._con)

    def tearDown(self) -> None:
        self.db.close()

    def test_raw_execute(self) -> None:
        self.assertEqual(self.db.raw_execute("SELECT COUNT(*) FROM routes;").one(), (3,))

    def assertAgencies(self, *agencies: str) -> None:
        self.assertSetEqual(
            set(cast(str, i[0]) for i in self.db.raw_execute("SELECT name FROM agencies;")),
            set(agencies),
        )

    def assertRoutes(self, *short_names: str) -> None:
        self.assertSetEqual(
            set(cast(str, i[0]) for i in self.db.raw_execute("SELECT short_name FROM routes;")),
            set(short_names),
        )

    def test_raw_execute_many(self) -> None:
        self.assertAgencies("Warszawska Kolej Dojazdowa")

        self.db.raw_execute_many(
            "INSERT INTO agencies (agency_id, name, url, timezone) VALUES (?, ?, ?, ?);",
            [
                ("1", "Foo", "https://example.com/", "UTC"),
                ("2", "Bar", "https://example.com/", "UTC"),
                ("3", "Baz", "https://example.com/", "UTC"),
            ],
        )

        self.assertAgencies("Warszawska Kolej Dojazdowa", "Foo", "Bar", "Baz")

    def test_typed_in_execute(self) -> None:
        self.assertAgencies("Warszawska Kolej Dojazdowa")
        self.db.typed_in_execute(
            "INSERT INTO :table VALUES :vals",
            Agency("1", "Foo", "https://example.com/", "UTC"),
        )
        self.assertAgencies("Warszawska Kolej Dojazdowa", "Foo")

    def test_typed_in_execute_many(self) -> None:
        self.assertAgencies("Warszawska Kolej Dojazdowa")
        self.db.typed_in_execute_many(
            "INSERT INTO :table VALUES :vals",
            Agency,
            [
                Agency("1", "Foo", "https://example.com/", "UTC"),
                Agency("2", "Bar", "https://example.com/", "UTC"),
            ],
        )
        self.assertAgencies("Warszawska Kolej Dojazdowa", "Foo", "Bar")

    def test_typed_out_execute(self) -> None:
        routes = self.db.typed_out_execute(
            "SELECT * FROM :table WHERE type = ?;",
            Route,
            (3,),
        ).all()
        self.assertSetEqual(set(i.short_name for i in routes), {"ZA1", "ZA12"})

    def test_retrieve(self) -> None:
        self.assertEqual(
            getattr(self.db.retrieve(Agency, "0"), "name", None),
            "Warszawska Kolej Dojazdowa",
        )
        self.assertIsNone(self.db.retrieve(Agency, "Missing"))

    def test_retrieve_must(self) -> None:
        self.assertEqual(
            self.db.retrieve_must(Agency, "0").name,
            "Warszawska Kolej Dojazdowa",
        )

        with self.assertRaises(EmptyQueryResult):
            self.db.retrieve_must(Agency, "Missing")

    def test_create(self) -> None:
        self.assertAgencies("Warszawska Kolej Dojazdowa")
        self.db.create(Agency("1", "Foo", "https://example.com/", "UTC"))
        self.assertAgencies("Warszawska Kolej Dojazdowa", "Foo")

    def test_create_many(self) -> None:
        self.assertAgencies("Warszawska Kolej Dojazdowa")
        self.db.create_many(
            Agency,
            [
                Agency("1", "Foo", "https://example.com/", "UTC"),
                Agency("2", "Bar", "https://example.com/", "UTC"),
            ],
        )
        self.assertAgencies("Warszawska Kolej Dojazdowa", "Foo", "Bar")

    def test_update(self) -> None:
        self.assertAgencies("Warszawska Kolej Dojazdowa")
        self.db.update(Agency("0", "WKD", "https://wkd.com.pl", "Europe/Warsaw"))
        self.assertAgencies("WKD")

    def test_update_many(self) -> None:
        self.assertRoutes("A1", "ZA1", "ZA12")
        self.db.update_many(
            Route,
            [
                Route("ZA1", "0", "A1-BUS", "", Route.Type.BUS),
                Route("ZA12", "0", "A12-BUS", "", Route.Type.BUS),
            ],
        )
        self.assertRoutes("A1", "A1-BUS", "A12-BUS")


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


class TestTypedQueryResult(unittest.TestCase):
    def setUp(self) -> None:
        self.db = DBConnection(":memory:")
        with sqlite3.Connection(FIXTURES / "wkd.db") as con:
            con.backup(self.db._con)

    def tearDown(self) -> None:
        self.db.close()

    def test_context_manager_closes(self) -> None:
        with self.db.typed_out_execute("SELECT * FROM agencies;", Agency) as cur:
            pass

        with self.assertRaises(sqlite3.ProgrammingError):
            cur.one()

    def test_iter(self) -> None:
        with self.db.typed_out_execute("SELECT * FROM routes;", Route) as cur:
            routes = list(cur)
            self.assertTrue(all(isinstance(i, Route) for i in routes))  # type: ignore
            self.assertListEqual([i.id for i in routes], ["A1", "ZA1", "ZA12"])

    def test_one(self) -> None:
        with self.db.typed_out_execute("SELECT * FROM agencies;", Agency) as cur:
            self.assertEqual(getattr(cur.one(), "name", None), "Warszawska Kolej Dojazdowa")
            self.assertIsNone(cur.one())

    def test_one_must(self) -> None:
        with self.db.typed_out_execute("SELECT * FROM agencies;", Agency) as cur:
            self.assertEqual(cur.one_must("first").name, "Warszawska Kolej Dojazdowa")
            with self.assertRaisesRegex(EmptyQueryResult, r"^second$"):
                cur.one_must("second")

    def test_many(self) -> None:
        with self.db.typed_out_execute("SELECT * FROM routes;", Route) as cur:
            all: list[str] = []
            while chunk := cur.many():
                all.extend(i.id for i in chunk)

            self.assertListEqual(all, ["A1", "ZA1", "ZA12"])

    def test_all(self) -> None:
        with self.db.typed_out_execute("SELECT * FROM routes;", Route) as cur:
            self.assertListEqual([i.id for i in cur.all()], ["A1", "ZA1", "ZA12"])
