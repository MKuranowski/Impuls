from typing import cast

from impuls import DBConnection, LocalResource, Pipeline, TaskRuntime
from impuls.model import Agency, FeedInfo, Route, Stop
from impuls.resource import ManagedResource
from impuls.tasks.merge import DatabaseToMerge, Merge, RouteHash, StopHash, pick_closest_stop
from impuls.tools.iteration import walk_len

from ..test_pipeline import DummyTask
from .template_testcase import FIXTURES_DIR, AbstractTestTask


class TestMergeIntoEmpty(AbstractTestTask.Template):
    db_name = None
    resources = {
        "wkd-old.db": LocalResource(FIXTURES_DIR / "wkd.db"),
        "wkd-new.db": LocalResource(FIXTURES_DIR / "wkd-next.db"),
    }

    def test(self) -> None:
        task = Merge([DatabaseToMerge("wkd-old.db", "1"), DatabaseToMerge("wkd-new.db", "2")])
        task.execute(self.runtime)

        # The single agency should be merged - it shares the same ID
        agencies = list(self.runtime.db.raw_execute("SELECT * FROM agencies"))
        self.assertListEqual(
            agencies,
            [
                (
                    "0",
                    "Warszawska Kolej Dojazdowa",
                    "http://www.wkd.com.pl/",
                    "Europe/Warsaw",
                    "pl",
                    "",
                    "",
                    None,
                ),
            ],
        )

        # The calendars should not be merged
        calendar_ids = list(
            cast(str, i[0])
            for i in self.runtime.db.raw_execute(
                "SELECT calendar_id FROM calendars ORDER BY calendar_id",
            )
        )
        self.assertListEqual(
            ["1:C", "1:D", "2:C", "2:D"],
            calendar_ids,
        )

        # Calendar exceptions shouldn't be merged as well
        calendar_exceptions = list(
            cast(str, i[0])
            for i in self.runtime.db.raw_execute("SELECT calendar_id FROM calendar_exceptions")
        )
        self.assertEqual(len(calendar_exceptions), 26)
        self.assertEqual(walk_len(i for i in calendar_exceptions if i.startswith("1:")), 14)
        self.assertEqual(walk_len(i for i in calendar_exceptions if i.startswith("2:")), 12)

        # Routes should be merged
        routes = list(self.runtime.db.raw_execute("SELECT * FROM routes ORDER BY route_id"))
        self.assertListEqual(
            routes,
            [
                (
                    "A1",
                    "0",
                    "A1",
                    "Warszawa Śródmieście WKD — Grodzisk Mazowiecki Radońska",
                    2,
                    "990099",
                    "FFFFFF",
                    None,
                    None,
                ),
                (
                    "ZA1",
                    "0",
                    "ZA1",
                    "Podkowa Leśna Główna — Grodzisk Mazowiecki Radońska (ZKA)",
                    3,
                    "990099",
                    "FFFFFF",
                    None,
                    None,
                ),
                (
                    "ZA12",
                    "0",
                    "ZA12",
                    "Podkowa Leśna Główna — Milanówek Grudów (ZKA)",
                    3,
                    "990099",
                    "FFFFFF",
                    None,
                    None,
                ),
            ],
        )

        # Stops should be merged
        stops = list(self.runtime.db.raw_execute("SELECT stop_id FROM stops ORDER BY stop_id"))
        self.assertListEqual(
            list(cast(str, i[0]) for i in stops),
            # cSpell: disable
            [
                "brzoz",
                "gmjor",
                "gmokr",
                "gmpia",
                "gmrad",
                "kanie",
                "kazim",
                "komor",
                "malic",
                "micha",
                "milgr",
                "nwwar",
                "opacz",
                "otreb",
                "plglo",
                "plwsc",
                "plzac",
                "poles",
                "prusz",
                "regul",
                "twork",
                "walje",
                "wocho",
                "wrako",
                "wreor",
                "wsalo",
                "wsrod",
                "wzach",
            ],
            # cSpell: enable
        )

        # Trips - should not be merged
        trips = list(
            cast(str, i[0]) for i in self.runtime.db.raw_execute("SELECT trip_id FROM trips")
        )
        self.assertEqual(len(trips), 744)
        self.assertEqual(walk_len(i for i in trips if i.startswith("1:")), 372)
        self.assertEqual(walk_len(i for i in trips if i.startswith("2:")), 372)

    def test_pre_merge_pipeline(self) -> None:
        dummy_task_old = DummyTask()
        dummy_task_new = DummyTask()

        task = Merge(
            [
                DatabaseToMerge("wkd-old.db", "1", Pipeline([dummy_task_old], name="Merge.Old")),
                DatabaseToMerge("wkd-new.db", "2", Pipeline([dummy_task_new], name="Merge.New")),
            ],
        )
        task.execute(self.runtime)

        # Check that the pre-merge pipelines were run
        self.assertEqual(dummy_task_old.executed_count, 1)
        self.assertEqual(dummy_task_new.executed_count, 1)


class TestMergeIntoExisting(AbstractTestTask.Template):
    db_name = "wkd.db"
    resources = {
        "wkd-new.db": LocalResource(FIXTURES_DIR / "wkd-next.db"),
    }

    def test(self) -> None:
        task = Merge([DatabaseToMerge("wkd-new.db", "1")])
        task.execute(self.runtime)

        # The single agency should be merged - it shares the same ID
        agencies = list(self.runtime.db.raw_execute("SELECT * FROM agencies"))
        self.assertListEqual(
            agencies,
            [
                (
                    "0",
                    "Warszawska Kolej Dojazdowa",
                    "http://www.wkd.com.pl/",
                    "Europe/Warsaw",
                    "pl",
                    "",
                    "",
                    None,
                ),
            ],
        )

        # The calendars should not be merged
        calendar_ids = list(
            cast(str, i[0])
            for i in self.runtime.db.raw_execute(
                "SELECT calendar_id FROM calendars ORDER BY calendar_id",
            )
        )
        self.assertListEqual(
            ["1:C", "1:D", "C", "D"],
            calendar_ids,
        )

        # Calendar exceptions shouldn't be merged as well
        calendar_exceptions = list(
            cast(str, i[0])
            for i in self.runtime.db.raw_execute("SELECT calendar_id FROM calendar_exceptions")
        )
        self.assertEqual(len(calendar_exceptions), 26)
        self.assertEqual(walk_len(i for i in calendar_exceptions if not i.startswith("1:")), 14)
        self.assertEqual(walk_len(i for i in calendar_exceptions if i.startswith("1:")), 12)

        # Routes should be merged
        routes = list(self.runtime.db.raw_execute("SELECT * FROM routes ORDER BY route_id"))
        self.assertListEqual(
            routes,
            [
                (
                    "A1",
                    "0",
                    "A1",
                    "Warszawa Śródmieście WKD — Grodzisk Mazowiecki Radońska",
                    2,
                    "990099",
                    "FFFFFF",
                    None,
                    None,
                ),
                (
                    "ZA1",
                    "0",
                    "ZA1",
                    "Podkowa Leśna Główna — Grodzisk Mazowiecki Radońska (ZKA)",
                    3,
                    "990099",
                    "FFFFFF",
                    None,
                    None,
                ),
                (
                    "ZA12",
                    "0",
                    "ZA12",
                    "Podkowa Leśna Główna — Milanówek Grudów (ZKA)",
                    3,
                    "990099",
                    "FFFFFF",
                    None,
                    None,
                ),
            ],
        )

        # Stops should be merged
        stops = list(self.runtime.db.raw_execute("SELECT stop_id FROM stops ORDER BY stop_id"))
        self.assertListEqual(
            list(cast(str, i[0]) for i in stops),
            # cSpell: disable
            [
                "brzoz",
                "gmjor",
                "gmokr",
                "gmpia",
                "gmrad",
                "kanie",
                "kazim",
                "komor",
                "malic",
                "micha",
                "milgr",
                "nwwar",
                "opacz",
                "otreb",
                "plglo",
                "plwsc",
                "plzac",
                "poles",
                "prusz",
                "regul",
                "twork",
                "walje",
                "wocho",
                "wrako",
                "wreor",
                "wsalo",
                "wsrod",
                "wzach",
            ],
            # cSpell: enable
        )

        # Trips - should not be merged
        trips = list(
            cast(str, i[0]) for i in self.runtime.db.raw_execute("SELECT trip_id FROM trips")
        )
        self.assertEqual(len(trips), 744)
        self.assertEqual(walk_len(i for i in trips if not i.startswith("1:")), 372)
        self.assertEqual(walk_len(i for i in trips if i.startswith("1:")), 372)

    def test_pre_merge_pipeline(self) -> None:
        dummy_task_new = DummyTask()
        task = Merge(
            [
                DatabaseToMerge("wkd-new.db", "2", Pipeline([dummy_task_new], name="Merge.New")),
            ],
        )
        task.execute(self.runtime)

        # Check that the pre-merge pipelines were run
        self.assertEqual(dummy_task_new.executed_count, 1)


class TestMergeRoutes(AbstractTestTask.Template):
    db_name = None
    agency = Agency(id="0", name="Example", url="https://example.com", timezone="UTC")

    def test_route_hash(self) -> None:
        r = Route(
            id="1",
            agency_id="0",
            short_name="A",
            long_name="Foo - Bar",
            type=Route.Type.BUS,
            color="000088",
            text_color="FFFFFF",
        )
        h = RouteHash.of(r)
        self.assertEqual(h.id, "1")
        self.assertEqual(h.agency_id, "0")
        self.assertEqual(h.short_name, "A")
        self.assertEqual(h.type, Route.Type.BUS)
        self.assertEqual(h.color, "000088")

    def test_similar_ids_and_hash(self) -> None:
        r1 = Route(
            id="1",
            agency_id=self.agency.id,
            short_name="A",
            long_name="Foo - Bar",
            type=Route.Type.BUS,
            color="000088",
            text_color="FFFFFF",
        )
        r2 = Route(
            id="1",
            agency_id=self.agency.id,
            short_name="A",
            long_name="Foo - Baz",
            type=Route.Type.BUS,
            color="000088",
            text_color="FFFFFF",
        )

        self.assertEqual(RouteHash.of(r1), RouteHash.of(r2))

        db1 = DBConnection.create_with_schema(self.workspace.path / "1.db")
        db1.create(self.agency)
        db1.create(r1)
        db2 = DBConnection.create_with_schema(self.workspace.path / "2.db")
        db2.create(self.agency)
        db2.create(r2)

        runtime = TaskRuntime(
            db=self.runtime.db,
            resources={
                "1.db": ManagedResource(self.workspace.path / "1.db"),
                "2.db": ManagedResource(self.workspace.path / "2.db"),
            },
            options=self.runtime.options,
        )
        task = Merge([DatabaseToMerge("1.db", "1"), DatabaseToMerge("2.db", "2")])
        task.execute(runtime)

        routes = list(self.runtime.db.raw_execute("SELECT * FROM routes"))
        self.assertListEqual(
            routes, [("1", "0", "A", "Foo - Bar", 3, "000088", "FFFFFF", None, None)]
        )

    def test_similar_ids_different_hash(self) -> None:
        r1 = Route(
            id="1",
            agency_id=self.agency.id,
            short_name="A",
            long_name="Foo - Bar",
            type=Route.Type.BUS,
            color="000088",
            text_color="FFFFFF",
        )
        r2 = Route(
            id="1",
            agency_id=self.agency.id,
            short_name="1",
            long_name="Spam - Eggs",
            type=Route.Type.TRAM,
            color="BB0000",
            text_color="FFFFFF",
        )

        self.assertNotEqual(RouteHash.of(r1), RouteHash.of(r2))

        db1 = DBConnection.create_with_schema(self.workspace.path / "1.db")
        db1.create(self.agency)
        db1.create(r1)
        db2 = DBConnection.create_with_schema(self.workspace.path / "2.db")
        db2.create(self.agency)
        db2.create(r2)

        runtime = TaskRuntime(
            db=self.runtime.db,
            resources={
                "1.db": ManagedResource(self.workspace.path / "1.db"),
                "2.db": ManagedResource(self.workspace.path / "2.db"),
            },
            options=self.runtime.options,
        )
        task = Merge([DatabaseToMerge("1.db", "1"), DatabaseToMerge("2.db", "2")])
        task.execute(runtime)

        routes = list(self.runtime.db.raw_execute("SELECT * FROM routes ORDER BY route_id"))
        self.assertListEqual(
            routes,
            [
                ("1", "0", "A", "Foo - Bar", 3, "000088", "FFFFFF", None, None),
                ("1:1", "0", "1", "Spam - Eggs", 0, "BB0000", "FFFFFF", None, None),
            ],
        )

    def test_different_ids(self) -> None:
        r1 = Route(
            id="1",
            agency_id=self.agency.id,
            short_name="A",
            long_name="Foo - Bar",
            type=Route.Type.BUS,
            color="000088",
            text_color="FFFFFF",
        )
        r2 = Route(
            id="A",
            agency_id=self.agency.id,
            short_name="A",
            long_name="Foo - Bar",
            type=Route.Type.BUS,
            color="000088",
            text_color="FFFFFF",
        )

        self.assertNotEqual(RouteHash.of(r1), RouteHash.of(r2))

        db1 = DBConnection.create_with_schema(self.workspace.path / "1.db")
        db1.create(self.agency)
        db1.create(r1)
        db2 = DBConnection.create_with_schema(self.workspace.path / "2.db")
        db2.create(self.agency)
        db2.create(r2)

        runtime = TaskRuntime(
            db=self.runtime.db,
            resources={
                "1.db": ManagedResource(self.workspace.path / "1.db"),
                "2.db": ManagedResource(self.workspace.path / "2.db"),
            },
            options=self.runtime.options,
        )
        task = Merge([DatabaseToMerge("1.db", "1"), DatabaseToMerge("2.db", "2")])
        task.execute(runtime)

        routes = list(self.runtime.db.raw_execute("SELECT * FROM routes ORDER BY route_id"))
        self.assertListEqual(
            routes,
            [
                ("1", "0", "A", "Foo - Bar", 3, "000088", "FFFFFF", None, None),
                ("A", "0", "A", "Foo - Bar", 3, "000088", "FFFFFF", None, None),
            ],
        )


class TestMergeStops(AbstractTestTask.Template):
    db_name = None

    def test_stop_hash(self) -> None:
        s = Stop(
            id="TYO",
            name="Tokyo",
            lat=35.68121,
            lon=139.76668,
            code="JY01",
            wheelchair_boarding=True,
        )
        h = StopHash.of(s)
        self.assertEqual(h.id, "TYO")
        self.assertEqual(h.name, "Tokyo")
        self.assertEqual(h.code, "JY01")
        self.assertEqual(h.zone_id, "")
        self.assertEqual(h.location_type, Stop.LocationType.STOP)
        self.assertEqual(h.parent_station, "")
        self.assertIs(h.wheelchair_boarding, True)
        self.assertEqual(h.platform_code, "")

    def test_pick_closest_stop(self) -> None:
        incoming = Stop(
            id="TYO",
            name="Tokyo",
            lat=35.68121,
            lon=139.76668,
            wheelchair_boarding=True,
            code="JY01",
        )

        self.assertIsNone(pick_closest_stop(incoming, [], 30.0))

        candidates = [
            Stop("TYO", "Tokyo", 35.68200, 139.76495, "JE01"),
            Stop("TYO", "Tokyo", 35.68208, 139.76564, "M-17"),
        ]
        self.assertIsNone(pick_closest_stop(incoming, candidates, 30.0))

        candidates = [
            Stop("TYO", "Tokyo", 35.68124, 139.76630, "JC01"),
            Stop("TYO", "Tokyo", 35.68111, 139.76697, "JT01"),
            Stop("TYO", "Tokyo", 35.68124, 139.76653, "JK01"),
            Stop("TYO", "Tokyo", 35.68200, 139.76495, "JE01"),
            Stop("TYO", "Tokyo", 35.68208, 139.76564, "M-17"),
        ]
        self.assertIs(pick_closest_stop(incoming, candidates, 30.0), candidates[2])

    def test_similar_ids_and_hash(self) -> None:
        s1 = Stop("TYO", "Tokyo", 35.68121, 139.76668)
        s2 = Stop("TYO", "Tokyo", 35.68124, 139.76653)
        self.assertEqual(StopHash.of(s1), StopHash.of(s2))

        db1 = DBConnection.create_with_schema(self.workspace.path / "1.db")
        db1.create(s1)
        db2 = DBConnection.create_with_schema(self.workspace.path / "2.db")
        db2.create(s2)

        runtime = TaskRuntime(
            db=self.runtime.db,
            resources={
                "1.db": ManagedResource(self.workspace.path / "1.db"),
                "2.db": ManagedResource(self.workspace.path / "2.db"),
            },
            options=self.runtime.options,
        )
        task = Merge(
            databases_to_merge=[DatabaseToMerge("1.db", "1"), DatabaseToMerge("2.db", "2")],
            distance_between_similar_stops_m=30.0,
        )
        task.execute(runtime)

        routes = list(self.runtime.db.raw_execute("SELECT stop_id, name, lat, lon FROM stops"))
        self.assertListEqual(routes, [("TYO", "Tokyo", 35.68121, 139.76668)])

    def test_similar_ids_and_hash_but_too_far(self) -> None:
        s1 = Stop("TYO", "Tokyo", 35.68121, 139.76668)
        s2 = Stop("TYO", "Tokyo", 35.682, 139.76495)
        self.assertEqual(StopHash.of(s1), StopHash.of(s2))

        db1 = DBConnection.create_with_schema(self.workspace.path / "1.db")
        db1.create(s1)
        db2 = DBConnection.create_with_schema(self.workspace.path / "2.db")
        db2.create(s2)

        runtime = TaskRuntime(
            db=self.runtime.db,
            resources={
                "1.db": ManagedResource(self.workspace.path / "1.db"),
                "2.db": ManagedResource(self.workspace.path / "2.db"),
            },
            options=self.runtime.options,
        )
        task = Merge(
            databases_to_merge=[DatabaseToMerge("1.db", "1"), DatabaseToMerge("2.db", "2")],
            distance_between_similar_stops_m=30.0,
        )
        task.execute(runtime)

        routes = list(self.runtime.db.raw_execute("SELECT stop_id, name, lat, lon FROM stops"))
        self.assertListEqual(
            routes,
            [("TYO", "Tokyo", 35.68121, 139.76668), ("TYO:1", "Tokyo", 35.682, 139.76495)],
        )

    def test_similar_ids_different_hash(self) -> None:
        s1 = Stop("TYO", "Tokyo", 35.68121, 139.76668, "JY01")
        s2 = Stop("TYO", "Tokyo", 35.68124, 139.76653, "JK01")
        self.assertNotEqual(StopHash.of(s1), StopHash.of(s2))

        db1 = DBConnection.create_with_schema(self.workspace.path / "1.db")
        db1.create(s1)
        db2 = DBConnection.create_with_schema(self.workspace.path / "2.db")
        db2.create(s2)

        runtime = TaskRuntime(
            db=self.runtime.db,
            resources={
                "1.db": ManagedResource(self.workspace.path / "1.db"),
                "2.db": ManagedResource(self.workspace.path / "2.db"),
            },
            options=self.runtime.options,
        )
        task = Merge(
            databases_to_merge=[DatabaseToMerge("1.db", "1"), DatabaseToMerge("2.db", "2")],
            distance_between_similar_stops_m=30.0,
        )
        task.execute(runtime)

        routes = list(
            self.runtime.db.raw_execute("SELECT stop_id, name, lat, lon, code FROM stops")
        )
        self.assertListEqual(
            routes,
            [
                ("TYO", "Tokyo", 35.68121, 139.76668, "JY01"),
                ("TYO:1", "Tokyo", 35.68124, 139.76653, "JK01"),
            ],
        )

    def test_different_ids(self) -> None:
        s1 = Stop("TYO", "Tokyo", 35.68121, 139.76668)
        s2 = Stop("JY01", "Tokyo", 35.68121, 139.76668)
        self.assertNotEqual(StopHash.of(s1), StopHash.of(s2))

        db1 = DBConnection.create_with_schema(self.workspace.path / "1.db")
        db1.create(s1)
        db2 = DBConnection.create_with_schema(self.workspace.path / "2.db")
        db2.create(s2)

        runtime = TaskRuntime(
            db=self.runtime.db,
            resources={
                "1.db": ManagedResource(self.workspace.path / "1.db"),
                "2.db": ManagedResource(self.workspace.path / "2.db"),
            },
            options=self.runtime.options,
        )
        task = Merge(
            databases_to_merge=[DatabaseToMerge("1.db", "1"), DatabaseToMerge("2.db", "2")],
            distance_between_similar_stops_m=30.0,
        )
        task.execute(runtime)

        routes = list(self.runtime.db.raw_execute("SELECT stop_id, name, lat, lon FROM stops"))
        self.assertListEqual(
            routes,
            [
                ("TYO", "Tokyo", 35.68121, 139.76668),
                ("JY01", "Tokyo", 35.68121, 139.76668),
            ],
        )


class TestMergeFeedInfo(AbstractTestTask.Template):
    db_name = None

    def test_existing(self) -> None:
        self.runtime.db.create(FeedInfo("Existing", "https://example.com", "en"))

        db1 = DBConnection.create_with_schema(self.workspace.path / "1.db")
        db1.create(FeedInfo("Incoming 1", "https://example.com", "en", "v1"))
        db2 = DBConnection.create_with_schema(self.workspace.path / "2.db")
        db2.create(FeedInfo("Incoming 2", "https://example.com", "en", "v2"))

        runtime = TaskRuntime(
            db=self.runtime.db,
            resources={
                "1.db": ManagedResource(self.workspace.path / "1.db"),
                "2.db": ManagedResource(self.workspace.path / "2.db"),
            },
            options=self.runtime.options,
        )
        task = Merge([DatabaseToMerge("1.db", "1"), DatabaseToMerge("2.db", "2")])
        task.execute(runtime)

        fi = self.runtime.db.retrieve_must(FeedInfo, "0")
        self.assertEqual(fi.publisher_name, "Existing")
        self.assertEqual(fi.publisher_url, "https://example.com")
        self.assertEqual(fi.lang, "en")
        self.assertEqual(fi.version, "")

    def test_all_incoming(self) -> None:
        db1 = DBConnection.create_with_schema(self.workspace.path / "1.db")
        db1.create(FeedInfo("Incoming 1", "https://example.com", "en", "v1"))
        db2 = DBConnection.create_with_schema(self.workspace.path / "2.db")
        db2.create(FeedInfo("Incoming 2", "https://example.com", "en", "v2"))

        runtime = TaskRuntime(
            db=self.runtime.db,
            resources={
                "1.db": ManagedResource(self.workspace.path / "1.db"),
                "2.db": ManagedResource(self.workspace.path / "2.db"),
            },
            options=self.runtime.options,
        )
        task = Merge([DatabaseToMerge("1.db", "1"), DatabaseToMerge("2.db", "2")])
        task.execute(runtime)

        fi = self.runtime.db.retrieve_must(FeedInfo, "0")
        self.assertEqual(fi.publisher_name, "Incoming 1")
        self.assertEqual(fi.publisher_url, "https://example.com")
        self.assertEqual(fi.lang, "en")
        self.assertEqual(fi.version, "v1/v2")

    def test_partial_incoming(self) -> None:
        db1 = DBConnection.create_with_schema(self.workspace.path / "1.db")
        db1.create(FeedInfo("Incoming 1", "https://example.com", "en", "v1"))
        DBConnection.create_with_schema(self.workspace.path / "2.db")

        runtime = TaskRuntime(
            db=self.runtime.db,
            resources={
                "1.db": ManagedResource(self.workspace.path / "1.db"),
                "2.db": ManagedResource(self.workspace.path / "2.db"),
            },
            options=self.runtime.options,
        )
        task = Merge([DatabaseToMerge("1.db", "1"), DatabaseToMerge("2.db", "2")])
        task.execute(runtime)

        self.assertIsNone(self.runtime.db.retrieve(FeedInfo, "0"))
