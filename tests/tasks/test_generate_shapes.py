# © Copyright 2026 Mikołaj Kuranowski
# SPDX-License-Identifier: GPL-3.0-or-later

import json
import logging
from collections.abc import Sequence
from pathlib import Path
from typing import Any, cast
from unittest import TestCase
from unittest.mock import Mock

from routx.wrapper import Graph

from impuls import selector
from impuls.model import Route
from impuls.resource import LocalResource
from impuls.task import TaskRuntime
from impuls.tasks.generate_shapes import (
    ErrorLogger,
    FallbackPolicy,
    GeneratedShape,
    GenerateShapes,
    GeoJSONErrorWriter,
    LegContext,
    LegPlanner,
    ShapePoint,
    ShapeValidator,
    StopSnapper,
    StraightLineSubstitute,
    Waypoint,
)
from impuls.tools.testing_mocks import MockFile

from .template_testcase import AbstractTestTask

FIXTURES = Path(__file__).with_name("fixtures")


class MockStopSnapper(StopSnapper):
    def snap(self, pt: Waypoint) -> int:
        return {
            "A": 10,
            "B": 20,
            "C": 30,
        }.get(pt.cache_key(), 0)


class TestHelpers(TestCase):
    def test_generated_shape_append_leg(self) -> None:
        shape = GeneratedShape()
        shape.append_leg(
            LegContext(Waypoint(("A", 0), 0.0, 0.0), Waypoint(("B", 1), 1.0, 1.0)),
            [
                (0.0, 0.0, 0.0),
                (0.2, 0.5, 1.0),
                (0.7, 0.5, 2.0),
                (1.0, 1.0, 3.0),
            ],
        )
        shape.append_leg(
            LegContext(Waypoint(("B", 1), 1.0, 1.0), Waypoint(("C", 2), 2.0, 2.0)),
            [
                (1.0, 1.0, 0.0),
                (1.5, 1.3, 0.3),
                (1.5, 1.6, 0.6),
                (2.0, 2.0, 0.9),
            ],
        )

        self.assertListEqual(
            shape.points,
            [
                (0.0, 0.0, 0.0),
                (0.2, 0.5, 1.0),
                (0.7, 0.5, 2.0),
                (1.0, 1.0, 3.0),
                (1.5, 1.3, 3.3),
                (1.5, 1.6, 3.6),
                (2.0, 2.0, 3.9),
            ],
        )

        self.assertDictEqual(shape.distances, {0: 0.0, 1: 3.0, 2: 3.9})

    def test_generated_shape_round(self) -> None:
        shape = GeneratedShape(
            points=[
                (0.0, 0.0, 0.0),
                (0.21328, 0.59317, 0.3412),
                (0.75497, 0.52539, 0.7882),
                (1.12102, 1.12347, 1.0126),
            ],
            distances={0: 0.0, 1: 0.5123, 2: 1.0126},
        )
        shape.round(coord_precision=2, dist_precision=1)

        self.assertListEqual(
            shape.points,
            [
                (0.0, 0.0, 0.0),
                (0.21, 0.59, 0.3),
                (0.75, 0.53, 0.8),
                (1.12, 1.12, 1.0),
            ],
        )

        self.assertDictEqual(shape.distances, {0: 0.0, 1: 0.5, 2: 1.0})

    def test_distance_limited_stop_snapper(self) -> None:
        s = MockStopSnapper().distance_limited(lambda _: (0.0, 0.0), max_distance_m=100.0)
        self.assertEqual(s.snap(Waypoint("A", 0.0001, 0.0001)), 10)
        self.assertEqual(s.snap(Waypoint("A", 0.1, 0.1)), 0)

    def test_cached_stop_snapper(self) -> None:
        upper = MockStopSnapper()
        upper.snap = Mock(wraps=upper.snap)

        s = upper.cached()
        self.assertDictEqual(s.cache, {})

        self.assertEqual(s.snap(Waypoint("A", 0.0001, 0.0001)), 10)
        self.assertEqual(s.snap(Waypoint("Z", 1.1, 1.1)), 0)
        self.assertDictEqual(s.cache, {"A": 10, "Z": 0})
        self.assertEqual(upper.snap.call_count, 2)

        self.assertEqual(s.snap(Waypoint("A", 1.1, 1.1)), 10)
        self.assertEqual(s.snap(Waypoint("Z", 0.0001, 0.0001)), 0)
        self.assertDictEqual(s.cache, {"A": 10, "Z": 0})
        self.assertEqual(upper.snap.call_count, 2)

    def test_error_logger(self) -> None:
        l = ErrorLogger()

        with self.assertLogs(l.logger, logging.WARNING) as log_ctx:
            l.on_error(
                LegContext(Waypoint(("A", 1), 0.0, 0.0), Waypoint(("B", 2), 1.0, 1.0, 42)),
                "unsnapped_waypoint",
                [],
            )

            l.on_error(
                LegContext(Waypoint(("B", 2), 0.0, 0.0, 37), Waypoint(("C", 3), 2.0, 2.0, 32)),
                {"error": "straight_line", "fit": 0.0001},
                [(1.0, 1.0, 0.0), (1.5, 1.3, 0.3), (1.5, 1.6, 0.6), (2.0, 2.0, 0.9)],
            )

        self.assertEqual(len(log_ctx.records), 2)
        self.assertEqual(
            log_ctx.records[0].message,
            "Shape from A to B failed to generate: unsnapped_waypoint",
        )
        self.assertEqual(
            log_ctx.records[1].message,
            "Shape from B to C failed to generate: {'error': 'straight_line', 'fit': 0.0001}",
        )

    def test_geojson_error_writer(self) -> None:
        with MockFile(directory=True) as temp_dir:
            l = GeoJSONErrorWriter(temp_dir)
            l.on_error(
                LegContext(Waypoint(("A", 1), 0.0, 0.0), Waypoint(("B", 2), 1.0, 1.0, 42)),
                "unsnapped_waypoint",
                [],
            )
            l.on_error(
                LegContext(Waypoint(("B", 2), 0.0, 0.0, 37), Waypoint(("C", 3), 2.0, 2.0, 32)),
                {"error": "straight_line", "fit": 0.0001},
                [(1.0, 1.0, 0.0), (1.5, 1.3, 0.3), (1.5, 1.6, 0.6), (2.0, 2.0, 0.9)],
            )

            self.assertSetEqual(
                {i.name for i in temp_dir.iterdir()},
                {"A__B.geojson", "B__C.geojson"},
            )

            with (temp_dir / "A__B.geojson").open("rb") as f:
                self.assertDictEqual(
                    json.load(f),
                    {
                        "type": "Feature",
                        "properties": {
                            "error": "unsnapped_waypoint",
                            "start_stop_id": "A",
                            "start_node_id": 0,
                            "end_stop_id": "B",
                            "end_node_id": 42,
                        },
                        "geometry": {
                            "type": "LineString",
                            "coordinates": [],
                        },
                    },
                )

            with (temp_dir / "B__C.geojson").open("rb") as f:
                self.assertDictEqual(
                    json.load(f),
                    {
                        "type": "Feature",
                        "properties": {
                            "error": {"error": "straight_line", "fit": 0.0001},
                            "start_stop_id": "B",
                            "start_node_id": 37,
                            "end_stop_id": "C",
                            "end_node_id": 32,
                        },
                        "geometry": {
                            "type": "LineString",
                            "coordinates": [[1.0, 1.0], [1.3, 1.5], [1.6, 1.5], [2.0, 2.0]],
                        },
                    },
                )

    def test_straight_line_substitute(self) -> None:
        s = StraightLineSubstitute()
        leg = s.substitute_leg(
            LegContext(
                Waypoint(("A", 1), 0.001, 0.002, 67),
                Waypoint(("B", 2), 0.0025, 0.0029, 69),
            ),
            "no_route",
        )
        assert isinstance(leg, list)
        self.assertListEqual(
            leg,
            [
                (0.001, 0.002, 0.0),
                (0.0025, 0.0029, 0.19451194911007125),
            ],
        )

    def test_leg_planner(self) -> None:
        start = Waypoint(("A", 0), 0.0, 0.0)
        end = Waypoint(("B", 1), 1.0, 1.0)
        legs = list(LegPlanner().plan_waypoints(start, end))
        self.assertListEqual(
            legs,
            [LegContext(Waypoint(("A", 0), 0.0, 0.0), Waypoint(("B", 1), 1.0, 1.0))],
        )


class MockShapeValidator(ShapeValidator):
    def validate_leg(self, ctx: LegContext, points: Sequence[ShapePoint]) -> Any:
        if ctx.end.cache_key() == "wsrod":
            return "no_warszawa_srodmiescie_wkd"
        return None


class FullGenerateShapes(GenerateShapes):
    def create_fallback_policy(self, r: TaskRuntime, graph: Graph) -> FallbackPolicy:
        return StraightLineSubstitute()

    def create_shape_validator(self, r: TaskRuntime, graph: Graph) -> ShapeValidator:
        return MockShapeValidator()


class TestGenerateShapes(AbstractTestTask.Template):
    resources = {  # noqa: RUF012
        "wkd-shape-graph.osm": LocalResource(FIXTURES / "wkd-shape-graph.osm"),
    }

    def setUp(self) -> None:
        super().setUp()
        with self.runtime.db.transaction():
            self.runtime.db.raw_execute("UPDATE trips SET shape_id = NULL")
            self.runtime.db.raw_execute("DELETE FROM shapes")

    def test(self) -> None:
        db = self.runtime.db
        task = FullGenerateShapes(
            osm_resource="wkd-shape-graph.osm",
            routes=selector.Routes(type=Route.Type.RAIL),
            id_prefix="generated_",
        )

        with self.assertLogs(task.logger, logging.WARNING) as log_ctx:
            task.execute(self.runtime)

        # Check that no shapes were generated for buses
        self.assertSetEqual(
            {
                cast(str | None, i[0])
                for i in db.raw_execute(
                    "SELECT shape_id FROM trips JOIN routes USING (route_id) WHERE type = 3"
                )
            },
            {None},
        )

        # There should be 4 unique shapes:
        # | Example Trip | From                   | To                     |
        # |--------------|------------------------|------------------------|
        # | C-303        | W-wa Śródmieście WKD   | Podkowa Leśna Główna   |
        # | C-105        | W-wa Śródmieście WKD   | Grodzisk Maz. Radońska |
        # | C-104        | Grodzisk Maz. Radońska | W-wa Śródmieście WKD   |
        # | C-312        | Podkowa Leśna Główna   | W-wa Śródmieście WKD   |
        self.assertEqual(
            cast(int, db.raw_execute("SELECT COUNT(*) FROM shapes").one_must("count")[0]),
            4,
        )

        # With 2 deliberate errors:
        # - no_route between Podkowa Leśna Zachodnia and Kazimierówka
        # - no_warszawa_srodmiescie_wkd from W-wa Ochota WKD to W-wa Śródmieście WKD
        self.assertSetEqual(
            {i.message for i in log_ctx.records},
            {
                "Shape from kazim to plzac failed to generate: no_route",
                "Shape from plzac to kazim failed to generate: no_route",
                "Shape from wocho to wsrod failed to generate: no_warszawa_srodmiescie_wkd",
            },
        )

        # Check the shape from Grodzisk to Warszawa

        # fmt: off
        shape_id = (
            db
            .raw_execute("SELECT shape_id FROM trips WHERE trip_id = 'C-104'")
            .one_must("C-104 trip missing")
            [0]
        )
        # fmt: on

        shape_points = [
            cast(tuple[float, float, float], i)
            for i in db.raw_execute(
                "SELECT lat, lon, shape_dist_traveled FROM shape_points "
                "WHERE shape_id = ? ORDER BY sequence",
                (shape_id,),
            )
        ]
        self.assertEqual(len(shape_points), 124)
        self.assertListEqual(
            shape_points[:6],
            [
                (52.100628, 20.628408, 0.0),
                (52.101208, 20.630243, 0.141),
                (52.101871, 20.632132, 0.29),
                (52.103317, 20.636744, 0.643),
                (52.104317, 20.639898, 0.886),
                (52.104633, 20.641272, 0.986),
            ],
        )
        self.assertListEqual(
            shape_points[-6:],
            [
                (52.221989, 20.975174, 30.509),
                (52.222267, 20.976322, 30.594),
                (52.222633, 20.977589, 30.689),
                (52.224277, 20.984509, 31.194),
                (52.225414, 20.989477, 31.556),
                (52.227686, 21.000404, 32.333),
            ],
        )

        shape_distances = {
            cast(str, i[0]): cast(float, i[1])
            for i in db.raw_execute(
                "SELECT stop_id, shape_dist_traveled FROM stop_times WHERE trip_id = 'C-104'"
            )
        }
        self.assertAlmostEqual(shape_distances["gmrad"], 0.0)
        self.assertAlmostEqual(shape_distances["gmjor"], 0.643)
        self.assertAlmostEqual(shape_distances["plzac"], 6.621)
        self.assertAlmostEqual(shape_distances["wocho"], 31.556)
        self.assertAlmostEqual(shape_distances["wsrod"], 32.333)
