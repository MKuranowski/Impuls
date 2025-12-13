import sqlite3
from typing import Type
from unittest import TestCase

from impuls.model import Transfer

from .template_entity import AbstractTestEntity


class TestTransfer(AbstractTestEntity.Template[Transfer]):
    def get_entity(self) -> Transfer:
        return Transfer(
            from_stop_id="S0",
            to_stop_id="S1",
            from_route_id="",
            to_route_id="",
            from_trip_id="T0",
            to_trip_id="T1",
            type=Transfer.Type.TIMED,
            id=1,
            extra_fields_json=None,
        )

    def get_type(self) -> Type[Transfer]:
        return Transfer

    def test_sql_marshall(self) -> None:
        self.assertTupleEqual(
            self.get_entity().sql_marshall(),
            ("S0", "S1", None, None, "T0", "T1", 1, None, None),
        )

    def test_sql_primary_key(self) -> None:
        self.assertTupleEqual(self.get_entity().sql_primary_key(), (1,))

    def test_sql_unmarshall(self) -> None:
        t = Transfer.sql_unmarshall((1, "S0", "S1", None, None, "T0", "T1", 1, None, None))

        self.assertEqual(t.id, 1)
        self.assertEqual(t.from_stop_id, "S0")
        self.assertEqual(t.to_stop_id, "S1")
        self.assertEqual(t.from_route_id, "")
        self.assertEqual(t.to_route_id, "")
        self.assertEqual(t.from_trip_id, "T0")
        self.assertEqual(t.to_trip_id, "T1")
        self.assertEqual(t.type, Transfer.Type.TIMED)
        self.assertIsNone(t.min_transfer_time)
        self.assertIsNone(t.extra_fields_json)


class TestTransferConstraints(TestCase):
    def setUp(self) -> None:
        self.db = sqlite3.connect(":memory:")
        self.db.isolation_level = None
        self.db.execute("PRAGMA foreign_keys=1")

        self.db.execute("CREATE TABLE stops (stop_id TEXT PRIMARY KEY) STRICT;")
        self.db.execute("CREATE TABLE routes (route_id TEXT PRIMARY KEY) STRICT;")
        self.db.execute("CREATE TABLE trips (trip_id TEXT PRIMARY KEY) STRICT;")
        self.db.executescript(Transfer.sql_create_table())

        self.db.execute("BEGIN TRANSACTION;")
        self.db.execute("INSERT INTO stops VALUES ('S0')")
        self.db.execute("INSERT INTO stops VALUES ('S1')")
        self.db.execute("INSERT INTO routes VALUES ('R0')")
        self.db.execute("INSERT INTO routes VALUES ('R1')")
        self.db.execute("INSERT INTO trips VALUES ('T0')")
        self.db.execute("INSERT INTO trips VALUES ('T1')")
        self.db.commit()

    def tearDown(self) -> None:
        self.db.close()

    def test_required_stop_id(self) -> None:
        self.db.execute(
            "INSERT INTO transfers (from_stop_id, to_stop_id, from_trip_id, to_trip_id, "
            "                       transfer_type, min_transfer_time) VALUES "
            "('S0', 'S1', NULL, NULL, 1, 300)"
        )

    def test_required_stop_ids_missing(self) -> None:
        with self.assertRaises(sqlite3.IntegrityError):
            self.db.execute(
                "INSERT INTO transfers (from_stop_id, to_stop_id, from_trip_id, to_trip_id, "
                "                       transfer_type, min_transfer_time) VALUES "
                "(NULL, NULL, 'T0', 'T1', 1, 300)"
            )

    def test_required_trip_id(self) -> None:
        self.db.execute(
            "INSERT INTO transfers (from_stop_id, to_stop_id, from_trip_id, to_trip_id, "
            "                       transfer_type) VALUES "
            "(NULL, NULL, 'T0', 'T1', 4)"
        )

    def test_required_trip_ids_missing(self) -> None:
        with self.assertRaises(sqlite3.IntegrityError):
            self.db.execute(
                "INSERT INTO transfers (from_stop_id, to_stop_id, from_trip_id, to_trip_id, "
                "                       transfer_type) VALUES "
                "('S0', 'S1', NULL, NULL, 4)"
            )
