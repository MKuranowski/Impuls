# © Copyright 2024 Mikołaj Kuranowski
# SPDX-License-Identifier: GPL-3.0-or-later

from operator import attrgetter
from pathlib import Path
from unittest import TestCase

from impuls import DBConnection, selector
from impuls.model import Route

FIXTURE = Path(__file__).with_name("tasks") / "fixtures" / "wkd.db"


class TestSelectorRoutes(TestCase):
    def setUp(self) -> None:
        self.db = DBConnection(FIXTURE)

    def test_all(self) -> None:
        self.assertSetEqual(
            set(selector.Routes().find_ids(self.db)),
            {"A1", "ZA1", "ZA12"},
        )

    def test_agency_id(self) -> None:
        self.assertSetEqual(
            set(selector.Routes(agency_id="0").find_ids(self.db)),
            {"A1", "ZA1", "ZA12"},
        )

    def test_type(self) -> None:
        self.assertSetEqual(
            set(selector.Routes(type=Route.Type.RAIL).find_ids(self.db)),
            {"A1"},
        )

    def test_type_ids(self) -> None:
        self.assertSetEqual(
            set(selector.Routes(type=Route.Type.BUS, ids={"A1", "ZA1", "FOO"}).find_ids(self.db)),
            {"ZA1"},
        )

    def test_objects(self) -> None:
        routes = sorted(selector.Routes().find(self.db), key=attrgetter("id"))
        self.assertEqual(len(routes), 3)

        self.assertEqual(routes[0].id, "A1")
        self.assertEqual(routes[0].short_name, "A1")
        self.assertEqual(routes[0].type, Route.Type.RAIL)

        self.assertEqual(routes[1].id, "ZA1")
        self.assertEqual(routes[1].short_name, "ZA1")
        self.assertEqual(routes[1].type, Route.Type.BUS)

        self.assertEqual(routes[2].id, "ZA12")
        self.assertEqual(routes[2].short_name, "ZA12")
        self.assertEqual(routes[2].type, Route.Type.BUS)

    def test_objects_filtered(self) -> None:
        routes = sorted(
            selector.Routes(type=Route.Type.BUS, ids={"A1", "ZA1"}).find(self.db),
            key=attrgetter("id"),
        )
        self.assertEqual(len(routes), 1)
        self.assertEqual(routes[0].id, "ZA1")
        self.assertEqual(routes[0].short_name, "ZA1")
        self.assertEqual(routes[0].type, Route.Type.BUS)
