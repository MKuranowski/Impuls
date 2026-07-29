# © Copyright 2026 Mikołaj Kuranowski
# SPDX-License-Identifier: GPL-3.0-or-later

import json
import logging
import shutil
from abc import ABC, abstractmethod
from collections import defaultdict
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import InitVar, dataclass, field
from itertools import pairwise
from math import isfinite
from pathlib import Path
from typing import Any, Generic, TypeAlias, TypeVar, cast

import routx
from typing_extensions import dataclass_transform

from .. import selector
from ..db import DBConnection
from ..model import Route
from ..task import Task, TaskRuntime
from ..tools.geo import earth_distance_m
from ..tools.types import Self, StrPath

StopKey: TypeAlias = tuple[str, int]
"""
Unique identifier of a stop within a trip - a :py:attr:`stop_id <impuls.model.Stop.id>` and
:py:attr:`stop_sequence <impuls.model.StopTime.stop_sequence>` pair.
"""

ShapeKey: TypeAlias = tuple[StopKey, ...]
"""Unique identifier of a shape - a variable-length tuple of :py:class:`stop keys <StopKey>`."""

ShapePoint: TypeAlias = tuple[float, float, float]
"""Point in a shape - a latitude, longitude and cumulative distance."""


@dataclass
class Waypoint:
    """Intermediary point used when generating a shape, usually corresponding to a stop."""

    id: StopKey | str
    """
    ID of the waypoint - either a :py:class:`stop <StopKey>`,
    or some arbitrary point described by a unique string, different to all stop ids.

    Both :py:attr:`StopKey.id` or arbitrary strings here can be used as
    cache keys in the same lookup table.
    """

    lat: float
    """Geographic latitude of the waypoint."""

    lon: float
    """Geographic longitude of the waypoint."""

    node: int = 0
    """
    Identifier of a node in a routing graph corresponding to this waypoint,
    or `0` when no appropriate node was found.
    """

    def cache_key(self) -> str:
        return self.id if isinstance(self.id, str) else self.id[0]


@dataclass
class LegContext:
    """
    All data necessary when generating a shape's leg between two
    :py:class:`waypoints <Waypoint>`.
    """

    start: Waypoint
    """Beginning of the leg."""

    end: Waypoint
    """End of the leg."""


@dataclass
class GeneratedShape:
    """Full shape, generated for a :py:class:`ShapeKey`."""

    points: list[ShapePoint] = field(default_factory=list[ShapePoint])
    """All :py:class:`points <ShapePoint>` of this shape."""

    distances: dict[int, float] = field(default_factory=dict[int, float])
    """
    Mapping from :py:attr:`stop_sequence <impuls.model.StopTime.stop_sequence>`
    to a cumulative distance along a shape to that stop.
    """

    def append_leg(self, ctx: LegContext, points: Iterable[ShapePoint]) -> None:
        """Adds a leg to this generated shape."""

        # If this is the very first leg, add the total distance to the very first stop
        if not self.points:
            if isinstance(ctx.start.id, tuple):
                self.distances[ctx.start.id[1]] = 0.0
            distance_offset = 0.0
        else:
            distance_offset = self.points[-1][2]

        # Unless this is the very first leg, don't copy the first point.
        # It'll be the same as the previous leg's last point, as that's a
        # node snapped to the same stop
        points_iter = iter(points)
        if self.points:
            next(points_iter, None)

        self.points.extend((lat, lon, distance_offset + dist) for (lat, lon, dist) in points_iter)

        if isinstance(ctx.end.id, tuple):
            self.distances[ctx.end.id[1]] = self.points[-1][2]

    def round(self, coord_precision: int = 6, dist_precision: int | None = 3) -> None:
        """Rounds all stored coordinates and distances to the provided number of decimal places."""
        dist_precision = coord_precision if dist_precision is None else dist_precision

        self.points = [
            (round(lat, coord_precision), round(lon, coord_precision), round(dist, dist_precision))
            for (lat, lon, dist) in self.points
        ]
        self.distances = {seq: round(dist, dist_precision) for seq, dist in self.distances.items()}


class LegRouter(ABC):
    """
    Abstract base class (interface) for a router - an object generating routes
    between :py:class:`two points <LegContext>`.
    """

    @abstractmethod
    def generate_leg(self, ctx: LegContext) -> Sequence[ShapePoint]:
        """
        Generates the shape between the two points in the provided :py:class:`context <LegContext>`.

        If it's not possible to reach end from start, or the route can't otherwise be generated
        (and the issue is to be suppressed), returns an empty sequence (`[]`).

        However, if the start and end were snapped to the same node, a sequence of length one
        (representing that node) should be returned.

        :py:class:`ShapeBuilder` ensures this method is not called with
        any :py:attr:`nodes <Waypoint.node>` set to ``0``.
        """
        raise NotImplementedError


class StopSnapper(ABC):
    """
    Abstract base class (interface) for a stop snapper - an object
    matching stops with nodes in a routing graph.
    """

    @abstractmethod
    def snap(self, pt: Waypoint) -> int:
        """
        Snaps a waypoint to a corresponding node in the routing graph, and returns that node's id.
        If there is no matched node, returns 0.

        This method should ignore any existing :py:attr:`Waypoint.node` and is **not** required
        to set that attribute back.
        """
        raise NotImplementedError

    def distance_limited(
        self,
        get_node_location: Callable[[int], tuple[float, float]],
        max_distance_m: float = 500.0,
    ) -> "DistanceLimitedStopSnapper":
        """
        Limits the maximum distance of waypoint-to-node matches
        by wrapping this snapper in a :py:class:`DistanceLimitedStopSnapper`.
        """
        return DistanceLimitedStopSnapper(self, get_node_location, max_distance_m)

    def cached(self) -> "CachedStopSnapper":
        """
        Caches all waypoint-to-node matches by wrapping this snapper
        in a :py:class:`CachedStopSnapper`.
        """
        return CachedStopSnapper(self)


class DistanceLimitedStopSnapper(StopSnapper):
    """
    A `decorator <https://en.wikipedia.org/wiki/Decorator_pattern>`_ :py:class:`StopSnapper`
    limiting snapped nodes to a maximum distance.
    """

    snapper: StopSnapper
    """Main :py:class:`StopSnapper`, doing the actual matching."""

    get_node_location: Callable[[int], tuple[float, float]]
    """Callback for retrieving the location of a node by its id."""

    max_distance: float
    """
    Max allowed distance to a node to allow a match.
    In meters, unless :py:meth:`distance` is overridden.
    """

    def __init__(
        self,
        snapper: StopSnapper,
        get_node_location: Callable[[int], tuple[float, float]],
        max_distance: float = 500.0,
    ) -> None:
        super().__init__()
        self.snapper = snapper
        self.get_node_location = get_node_location
        self.max_distance = max_distance

    def snap(self, pt: Waypoint) -> int:
        id = self.snapper.snap(pt)
        if id == 0:
            return 0

        node_lat, node_lon = self.get_node_location(id)
        if self.distance(pt.lat, pt.lon, node_lat, node_lon) > self.max_distance:
            return 0

        return id

    def distance(self, pt_lat: float, pt_lon: float, node_lat: float, node_lon: float) -> float:
        """
        Calculates the distance between two points, for comparing against :py:attr:`max_distance`.

        Defaults to meters, as calculated by :py:func:`impuls.tools.geo.earth_distance_m`.
        """
        return earth_distance_m(pt_lat, pt_lon, node_lat, node_lon)

    def distance_limited(
        self,
        get_node_location: Callable[[int], tuple[float, float]],
        max_distance_m: float = 500,
    ) -> Self:
        """Modifies attributes of this distance-limiting decorator."""
        self.get_node_location = get_node_location
        self.max_distance = max_distance_m
        return self


class CachedStopSnapper(StopSnapper):
    """
    A `decorator <https://en.wikipedia.org/wiki/Decorator_pattern>`_ :py:class:`StopSnapper`
    caching the results of matching.
    """

    snapper: StopSnapper
    """Main :py:class:`StopSnapper`, doing the actual matching."""

    cache: dict[str, int]
    """Cache from a :py:class:`Waypoint` key to matched node."""

    def __init__(self, snapper: StopSnapper) -> None:
        super().__init__()
        self.snapper = snapper
        self.cache = {}

    def snap(self, pt: Waypoint) -> int:
        key = pt.cache_key()
        if (node_id := self.cache.get(key)) is None:
            node_id = self.snapper.snap(pt)
            self.cache[key] = node_id
        return node_id

    def cached(self) -> Self:
        """Returns this snapper unmodified."""
        return self


class ErrorObserver:
    """
    Base class for error observers - objects collecting information on errors encountered
    during shape generation.

    The default implementation does nothing.
    """

    def on_error(self, ctx: LegContext, error: Any, points: Sequence[ShapePoint]) -> None:
        """Callback on an error."""


class MultiErrorObserver(ErrorObserver):
    """MultiErrorObserver is an :py:class:`ErrorObserver` delegating an error callback to
    multiple other observers.
    """

    observers: tuple[ErrorObserver, ...]

    def __init__(self, *observers: ErrorObserver) -> None:
        self.observers = observers

    def on_error(self, ctx: LegContext, error: Any, points: Sequence[ShapePoint]) -> None:
        for observer in self.observers:
            observer.on_error(ctx, error, points)


class ErrorLogger(ErrorObserver):
    """:py:class:`ErrorObserver` implementation which logs all errors as warnings."""

    logger: logging.Logger

    def __init__(self, logger: logging.Logger | None) -> None:
        self.logger = logger or logging.getLogger("Task.GenerateShapes")

    def on_error(self, ctx: LegContext, error: Any, points: Sequence[ShapePoint]) -> None:
        self.logger.warning(
            "Shape from %s to %s failed to generate: %s",
            ctx.start.cache_key(),
            ctx.end.cache_key(),
            error,
        )


class GeoJSONErrorWriter(ErrorObserver):
    """
    :py:class:`ErrorObserver` implementation which dumps all shape generation errors
    as GeoJSON to the provided directory.

    All error reasons must be serializable as JSON.
    """

    directory: Path

    def __init__(self, directory: StrPath, /, clear: bool = False) -> None:
        self.directory = Path(directory)

        if clear:
            for f in self.directory.iterdir():
                if f.is_dir():
                    shutil.rmtree(f)
                else:
                    f.unlink()

    def context_to_filename(self, ctx: LegContext, error: Any) -> str:
        start_key = ctx.start.cache_key()
        end_key = ctx.end.cache_key()
        return f"{start_key}__{end_key}.geojson"

    def on_error(self, ctx: LegContext, error: Any, points: Sequence[ShapePoint]) -> None:
        file_name = self.context_to_filename(ctx, error)
        file_path = self.directory / file_name
        with file_path.open("w", encoding="utf-8") as f:
            json.dump(
                {
                    "type": "Feature",
                    "properties": {
                        "error": error,
                        "start_stop_id": ctx.start.cache_key(),
                        "start_node_id": ctx.start.node,
                        "end_stop_id": ctx.end.cache_key(),
                        "end_node_id": ctx.end.node,
                    },
                    "geometry": {
                        "type": "LineString",
                        "coordinates": [[lon, lat] for (lat, lon, _) in points],
                    },
                },
                f,
                ensure_ascii=False,
                indent=2,
            )


class FallbackPolicy:
    """
    Base class for a fallback policy - objects which decide what to do when
    a shape's leg fails to be generated.

    The default implementation returns no substitutions, aborting entire shapes.
    """

    def substitute_leg(self, ctx: LegContext, error: Any) -> Sequence[ShapePoint]:
        """
        Return a substitute shape for the provided leg;
        or an empty sequence to completely abandon the entire shape.
        """
        return []


class StraightLineSubstitute(FallbackPolicy):
    """
    A :py:class:`FallbackPolicy` which substitutes failed shape segments by straight lines
    between waypoints.
    """

    def distance(self, start: Waypoint, end: Waypoint) -> float:
        """
        Computes the distance between two waypoints, to match with
        :py:class:`LegRouter` units. Defaults to :py:func:`impuls.tools.geo.earth_distance_m`
        converted to **kilometers**.
        """
        return earth_distance_m(start.lat, start.lon, end.lat, end.lon) / 1000

    def substitute_leg(self, ctx: LegContext, error: Any) -> Sequence[ShapePoint]:
        dist = self.distance(ctx.start, ctx.end)
        return [(ctx.start.lat, ctx.start.lon, 0.0), (ctx.end.lat, ctx.end.lon, dist)]


class LegPlanner:
    """
    Base class for leg planners - objects which decide how a route between two
    :py:class:`waypoints <Waypoint>` is generated; in particular if any extra waypoints
    should be inserted.

    The root planner (the one returned by
    :py:meth:`AbstractGenerateShapes.create_leg_planner`) is only called with both waypoints
    representing stops; but that might not hold if planners are composed/nested.

    The default implementation doesn't add any waypoints, and simply returns
    a single `LegContext(start, end)`.
    """

    def plan_waypoints(self, start: Waypoint, end: Waypoint) -> Iterable[LegContext]:
        return [LegContext(start, end)]


class ShapeValidator:
    """
    Base class for shape validators - objects which decide if a generate shape leg looks correct.

    The default implementation allows all shapes.
    """

    def validate_leg(self, ctx: LegContext, points: Sequence[ShapePoint]) -> Any:
        """
        Checks if a shape segment looks correct.

        If the shape leg looks correct, returns a false-y value, preferably None.

        Otherwise, returns a truthy value representing the reason why the shape
        looks incorrect. Users are encouraged to keep return values JSON serializable,
        especially strings.
        """
        return None


class MultiShapeValidator(ShapeValidator):
    """
    MultiShapeValidator is a :py:class`ShapeValidator` delegating the validation to multiple other
    validators, returning the first encountered error (short circuiting), if any.
    """

    validators: tuple[ShapeValidator, ...]

    def __init__(self, *validators: ShapeValidator) -> None:
        self.validators = validators

    def validate_leg(self, ctx: LegContext, points: Sequence[ShapePoint]) -> Any:
        for validator in self.validators:
            if error := validator.validate_leg(ctx, points):
                return error
        return None


@dataclass
class ShapeBuilder:
    """
    ShapeBuilder orchestrates all necessary single-responsibility
    objects to actually build and generate shapes.

    Users are not really supposed to subclass the builder,
    if such a necessity arises, please file an issue.
    """

    snapper: StopSnapper
    planner: LegPlanner
    router: LegRouter
    validator: ShapeValidator
    fallback: FallbackPolicy
    observer: ErrorObserver
    stop_locations: Mapping[str, tuple[float, float]]

    def build(self, stops: Iterable[StopKey]) -> GeneratedShape | None:
        """Builds a shape between the provided stops."""
        shape = GeneratedShape()

        for leg_ctx in self.get_legs(stops):
            if leg_points := self.build_leg(leg_ctx):
                shape.append_leg(leg_ctx, leg_points)
            else:
                return None

        shape.round()
        return shape

    def get_legs(self, stops: Iterable[StopKey]) -> list[LegContext]:
        """Gets all legs of a shape, by querying the :py:class:`LegPlanner` for every stop pair."""
        return [
            leg
            for stop_a, stop_b in pairwise(stops)
            for leg in self.planner.plan_waypoints(
                self.stop_to_waypoint(stop_a),
                self.stop_to_waypoint(stop_b),
            )
        ]

    def build_leg(self, ctx: LegContext) -> Sequence[ShapePoint]:
        """
        Builds a shape between two waypoints, by calling the :py:class:`StopSnapper`
        and :py:class:`LegRouter`; and validating the segment with :py:class:`ShapeValidator`.

        If the shape fails to generate or is deemed incorrect, calls the :py:class:`ErrorObserver`
        and :py:class:`FallbackPolicy`.
        """
        # Snap stops to nodes
        ctx.start.node = self.snapper.snap(ctx.start)
        ctx.end.node = self.snapper.snap(ctx.end)
        if ctx.start.node == 0 or ctx.end.node == 0:
            return self.on_error(ctx, "unsnapped_waypoint", [])

        # Generate the route
        points = self.router.generate_leg(ctx)
        if not points:
            return self.on_error(ctx, "no_route", points)
        elif error := self.validator.validate_leg(ctx, points):
            return self.on_error(ctx, error, points)
        return points

    def on_error(
        self,
        ctx: LegContext,
        error: Any,
        points: Sequence[ShapePoint],
    ) -> Sequence[ShapePoint]:
        """
        Deals with a shape error - invokes the :py:class:`ErrorObserver`
        and gets the fallback shape from the :py:class:`FallbackPolicy`.
        """
        self.observer.on_error(ctx, error, points)
        return self.fallback.substitute_leg(ctx, error)

    def stop_to_waypoint(self, stop: StopKey) -> Waypoint:
        lat, lon = self.stop_locations[stop[0]]
        return Waypoint(id=stop, lat=lat, lon=lon)


GraphT = TypeVar("GraphT")


@dataclass_transform(kw_only_default=True, eq_default=False)
@dataclass(kw_only=True, repr=False, eq=False)
class AbstractGenerateShapes(Task, Generic[GraphT]):
    """
    Base abstract class for generating shapes, agnostic to any "routing graphs".

    This class is mostly responsible for negotiating with the database -
    querying for trips, inserting shapes and updating shape_dist_traveled,
    but it also generates the shapes using the quasi-private :py:class:`ShapeBuilder`.

    Responsibility for providing the routing graph and other required lays on
    the programmer. Implementing shape generation involves
    `injecting dependencies <https://en.wikipedia.org/wiki/Dependency_injection>`_
    by overriding the :py:meth:`create_routing_graph`, :py:meth:`create_leg_router`,
    :py:meth:`create_stop_snapper` and possibly other ``create_xxx`` methods.

    AbstractGenerateShapes is a `kw-only dataclass <https://docs.python.org/3/library/dataclasses.html>`_,
    in order to make adding new options/settings as easy as possible, without having
    to write out enormous ``__init__`` methods manually. All subclasses are automatically
    wrapped in `@dataclass` though a `__init_subclass__ <https://docs.python.org/3/reference/datamodel.html#object.__init_subclass__>`_
    hook. To add new options, simply do::

        class MyGenerateShapes(AbstractGenerateShapes[...]):
            custom_knob: float
            another_parameter_with_default: int = 42

    And those parameters will automatically be added as parameters to the ``__init__``
    method of MyGenerateShapes. `InitVar <https://docs.python.org/3/library/dataclasses.html#dataclasses.InitVar>`_
    is more convoluted, as the base class uses one for the task name::

        class MyGenerateShapes(AbstractGenerateShapes[...]):
            custom_knob: InitVar[float | None] = None
            custom_knob_resolved: float = field(init=False)

            def __post_init__(self, task_name: str | None, custom_knob: float | None) -> None:
                super().__post_init__(task_name)
                self.custom_knob_resolved = custom_knob or fallback_custom_knob()

    Of course, if you don't want to add new options, you don't need to worry about all that.
    """

    routes: selector.Routes
    """Selector for which routes shapes should be generated."""

    id_prefix: str
    """
    Prefix for generated shape IDs; which are consecutive integers.

    Users must ensure the prefix guarantees shape ID uniqueness,
    particularly if using generate shapes multiple times,
    or if the database already has shapes.
    """

    overwrite: bool = False
    """
    Should any existing shapes (in selected routes) be overwritten?

    Note that this only replaces :py:attr:`Trip.shape_id <impuls.model.Trip.shape_id>`,
    without removing shapes; as those shapes may be used by unselected trips.
    Vacuum unused shapes afterwards with an :py:class:`ExecuteSQL <impuls.tasks.ExecuteSQL>`
    or a :py:class:`RemoveUnusedEntities <impuls.tasks.RemoveUnusedEntities>` task.

    Defaults to ``False``.
    """

    progress_report_step: int = 100
    """
    How often should an info message be logged with shape generation progress?

    Defaults to ``100`` - every 100th shape causes an info message to be logged.
    All other generated shapes cause a debug logging message.
    """

    task_name: InitVar[str | None] = None

    def __post_init__(self, task_name: str | None) -> None:
        super().__init__(task_name)

    def __init_subclass__(cls) -> None:
        super().__init_subclass__()
        dataclass(kw_only=True, repr=False, eq=False)(cls)

    @abstractmethod
    def create_routing_graph(self, r: TaskRuntime) -> GraphT:
        """
        Instantiates a routing graph, which will be passed along to other shape-building classes.
        """
        raise NotImplementedError

    @abstractmethod
    def create_stop_snapper(self, r: TaskRuntime, graph: GraphT) -> StopSnapper:
        """
        Instantiates a :py:class:`StopSnapper`, used to match :py:class:`Waypoints <Waypoint>`
        with nodes in the routing graph.
        """
        raise NotImplementedError

    @abstractmethod
    def create_leg_router(self, r: TaskRuntime, graph: GraphT) -> LegRouter:
        """
        Instantiates a :py:class:`LegRouter`, used to generate shape segments between
        two :py:class:`Waypoints <Waypoint>`.
        """
        raise NotImplementedError

    def create_leg_planner(self, r: TaskRuntime, graph: GraphT) -> LegPlanner:
        """
        Instantiates a :py:class:`LegPlanner`, used to generate all waypoints
        to traverse between two stops.

        The default implementation simply returns no waypoints; converting the stop pair to
        a single :py:class:`leg <LegContext>`.
        """
        return LegPlanner()

    def create_shape_validator(self, r: TaskRuntime, graph: GraphT) -> ShapeValidator:
        """
        Instantiates a :py:class:`ShapeValidator`, used to determine if
        a shape segment looks correct.

        The default implementation allows any shapes.
        """
        return ShapeValidator()

    def create_fallback_policy(self, r: TaskRuntime, graph: GraphT) -> FallbackPolicy:
        """
        Instantiates a :py:class:`FallbackPolicy`, used to determine what to do when
        a shape segment fails to generate.

        The default implementation aborts the entire shape.
        """
        return FallbackPolicy()

    def create_error_observer(self, r: TaskRuntime, graph: GraphT) -> ErrorObserver:
        """
        Instantiates an :py:class:`ErrorObserver`, a callback when shape segment fails to generate.

        The default implementation :py:class:`logs failures as warnings <ErrorLogger>`.
        """
        return ErrorLogger(self.logger)

    def create_shape_builder(self, r: TaskRuntime, graph: GraphT) -> ShapeBuilder:
        """
        Instantiates a :py:class:`ShapeBuilder`, by calling all other ``create_xxx``` methods.

        This should not really by overridden - if there's a need for that, file an issue.
        """
        return ShapeBuilder(
            snapper=self.create_stop_snapper(r, graph),
            planner=self.create_leg_planner(r, graph),
            router=self.create_leg_router(r, graph),
            validator=self.create_shape_validator(r, graph),
            fallback=self.create_fallback_policy(r, graph),
            observer=self.create_error_observer(r, graph),
            stop_locations=self.get_stop_locations(r),
        )

    def execute(self, r: TaskRuntime) -> None:
        """
        Generates shapes by:

        1. gathering trips to process,
        2. creating the routing graph,
        3. creating the shape builder,
        4. generating shapes and inserting the into the database.
        """

        self.logger.info("Getting trips to process")
        trip_ids = self.get_trips_to_process(r.db)
        if not trip_ids:
            self.logger.warning("No trips to process")
            return

        grouped_trips = self.group_trips(r.db, trip_ids)
        self.remove_existing_shapes(r.db, trip_ids)

        self.logger.info("Creating the routing graph")
        graph = self.create_routing_graph(r)

        self.logger.info("Creating the shape builder")
        builder = self.create_shape_builder(r, graph)

        self.generate_shapes(r.db, grouped_trips, builder)
        self.logger.info("Shape generation completed")

    def get_trips_to_process(self, db: DBConnection) -> Sequence[str]:
        """
        Gets identifiers of all trips to generate shapes for.

        This returns all trips of routes selected by :py:attr:`the provided selector <routes>`;
        that don't already have shapes (``shape_id IS NULL``) unless :py:attr:`Options.overwrite`
        is set.
        """

        route_ids = list(self.routes.find_ids(db))
        query = (
            "SELECT trip_id FROM trips WHERE route_id = ?"
            if self.overwrite
            else "SELECT trip_id FROM trips WHERE route_id = ? AND shape_id IS NULL"
        )
        return [
            cast(str, row[0])
            for route_id in route_ids
            for row in db.raw_execute(query, (route_id,))
        ]

    def remove_existing_shapes(self, db: DBConnection, trip_ids: Iterable[str]) -> None:
        """Removes shapes of the provided trips, but only if :py:attr:`Options.overwrite` is set."""
        if self.overwrite:
            with db.transaction():
                db.raw_execute_many(
                    "UPDATE trips SET shape_id = NULL WHERE trip_id = ?",
                    ((trip_id,) for trip_id in trip_ids),
                )

    def generate_shapes(
        self,
        db: DBConnection,
        grouped_trips: Mapping[ShapeKey, Sequence[str]],
        builder: ShapeBuilder,
    ) -> None:
        """
        Generates and :py:meth:`assigns <assign_shape>` shapes for the provided,
        already grouped trips.
        """

        total = len(grouped_trips)
        for idx, (stops, trip_ids) in enumerate(grouped_trips.items()):
            (self.logger.info if idx % self.progress_report_step == 0 else self.logger.debug)(
                "Generating shape %d/%d (%.2f %%)",
                idx,
                total,
                100 * idx / total,
            )

            shape_id = f"{self.id_prefix}{idx}"
            shape = builder.build(stops)
            if shape:
                self.assign_shape(db, shape_id, shape, trip_ids)

    def assign_shape(
        self,
        db: DBConnection,
        shape_id: str,
        shape: GeneratedShape,
        trip_ids: Sequence[str],
    ) -> None:
        """
        Inserts the provided shape and assigns it to the trips.

        This amounts to 4 different operations, wrapped in a transaction:

        1. ``INSERT INTO shapes``,
        2. ``INSERT INTO shape_points``,
        3. ``UPDATE trips SET shape_id``,
        4. ``UPDATE stop_times SET shape_dist_traveled``.
        """

        with db.transaction():
            db.raw_execute("INSERT INTO shapes (shape_id) VALUES (?)", (shape_id,))
            db.raw_execute_many(
                "INSERT INTO shape_points (shape_id, sequence, lat, lon, shape_dist_traveled) "
                "VALUES (?, ?, ?, ?, ?)",
                (
                    (shape_id, idx, lat, lon, dist)
                    for idx, (lat, lon, dist) in enumerate(shape.points)
                ),
            )
            db.raw_execute_many(
                "UPDATE trips SET shape_id = ? WHERE trip_id = ?",
                ((shape_id, trip_id) for trip_id in trip_ids),
            )
            db.raw_execute_many(
                "UPDATE stop_times SET shape_dist_traveled = ? "
                "WHERE trip_id = ? AND stop_sequence = ?",
                (
                    (dist, trip_id, idx)
                    for trip_id in trip_ids
                    for idx, dist in shape.distances.items()
                ),
            )

    def group_trips(
        self,
        db: DBConnection,
        trip_ids: Iterable[str],
    ) -> Mapping[ShapeKey, Sequence[str]]:
        """
        Groups the provided trips by the same :py:class:`ShapeKey` - sequence of stops.

        Trips' shape keys are retrieved by calling :py:meth:`get_trip_stops`.
        """
        by_stops = defaultdict[ShapeKey, list[str]](list)
        for trip_id in trip_ids:
            trip_stops = self.get_trip_stops(db, trip_id)
            by_stops[trip_stops].append(trip_id)
        return by_stops

    def get_trip_stops(self, db: DBConnection, trip_id: str) -> ShapeKey:
        """
        Retrieves all stops of a trip - its :py:class:`ShapeKey`.

        The default implementation simply executes::

            SELECT stop_id, stop_sequence
            FROM stop_times
            WHERE trip_id = ?
            ORDER BY stop_sequence ASC
        """

        return tuple(
            (cast(str, i[0]), cast(int, i[1]))
            for i in db.raw_execute(
                "SELECT stop_id, stop_sequence FROM stop_times "
                "WHERE trip_id = ? ORDER BY stop_sequence ASC",
                (trip_id,),
            )
        )

    def get_stop_locations(self, r: TaskRuntime) -> dict[str, tuple[float, float]]:
        """
        Gets positions of all stops, for snapping.

        The default implementation simply calls ``SELECT stop_id, lat, lon FROM stops``,
        as that's significantly easier than actually filtering out used stops,
        without increasing memory that much.
        """
        with r.db.raw_execute("SELECT stop_id, lat, lon FROM stops") as query:
            rows = cast(Iterable[tuple[str, float, float]], query)
            return {i[0]: (i[1], i[2]) for i in rows}


class RoutxKDTreeStopSnapper(StopSnapper):
    """
    Snap stops to a `routx routing graph <https://pypi.org/project/routx/#user-content-routxgraph>`_
    using a `k-d tree <https://pypi.org/project/routx/#user-content-routxkdtree>`_.
    """

    kd_tree: routx.KDTree

    def __init__(self, graph: routx.Graph) -> None:
        super().__init__()
        self.kd_tree = routx.KDTree.build(graph)

    def snap(self, pt: Waypoint) -> int:
        return self.kd_tree.find_nearest_node(pt.lat, pt.lon).id


class RoutxLegRouter(LegRouter):
    """
    Generates routes on a
    `routx routing graph <https://pypi.org/project/routx/#user-content-routxgraph>`_;
    simplifying the results with the
    `Ramer-Douglas-Peucker algorithm <https://en.wikipedia.org/wiki/Ramer%E2%80%93Douglas%E2%80%93Peucker_algorithm>`_.
    """

    graph: routx.Graph

    simplify_epsilon: float
    """
    Threshold, in decimal degrees, for the `RDP simplification algorithm <https://en.wikipedia.org/wiki/Ramer%E2%80%93Douglas%E2%80%93Peucker_algorithm>`_
    to determine if a node "sticks out" enough to be considered important and preserved.

    Defaults to ``1e-5``, but can be set to a non-finite number (e.g. ``nan``)
    to disable simplification entirely.
    """

    def __init__(self, graph: routx.Graph, simplify_epsilon: float = 1e-5) -> None:
        super().__init__()
        self.graph = graph
        self.simplify_epsilon = simplify_epsilon

    def generate_leg(self, ctx: LegContext) -> Sequence[ShapePoint]:
        assert ctx.start.node != 0
        assert ctx.end.node != 0

        try:
            nodes = self.graph.find_route(ctx.start.node, ctx.end.node)
        except routx.StepLimitExceeded:
            return []

        if isfinite(self.simplify_epsilon):
            nodes = self.graph.simplify_route(nodes, self.simplify_epsilon)

        return self.nodes_to_shape_points(nodes)

    def nodes_to_shape_points(self, nodes: Iterable[int]) -> list[ShapePoint]:
        points = list[ShapePoint]()
        for node_id in nodes:
            node = self.graph[node_id]
            if points:
                dist = points[-1][2] + routx.earth_distance(
                    points[-1][0],
                    points[-1][1],
                    node.lat,
                    node.lon,
                )
            else:
                dist = 0.0
            points.append((node.lat, node.lon, dist))
        return points


class GenerateShapes(AbstractGenerateShapes[routx.Graph]):
    """
    Generates shapes based on an routing data stored in an
    `OpenStreetMap XML <https://wiki.openstreetmap.org/wiki/OSM_XML>`_ or
    an `OpenStreetMap PBF <https://wiki.openstreetmap.org/wiki/PBF_Format>`_ file
    using the `routx library <https://pypi.org/project/routx/>`_.

    Note that using actual data from `OpenStreetMap <https://www.openstreetmap.org/#map=15/51.91770/-2.57938>`_
    like `Geofabrik data extracts <https://download.geofabrik.de/>`_ is
    `licensed under ODbL <https://www.openstreetmap.org/copyright/>`_; making your resulting file
    subject to appropriate attributions.

    For simple, fixed-track networks (anything rail-based), users are encouraged to draw
    their own routing graphs using tools like `JOSM <https://josm.eu/>`_.

    Each stop is snapped to its closest node in the routing graph, and then
    shortest routes between each pair of stops are found using the
    `A* algorithm <https://en.wikipedia.org/wiki/A*_search_algorithm>`_. Note that this might
    not actually be desirable, especially with hyper-mapped rail data, where every single
    track is mapped as a separate way. If the node distribution and stop position align
    just right, a stop might be snapped to a way in the opposite direction, resulting in wonky
    shapes.

    This implementation doesn't do any validation of generated shapes, nor
    does it prevent abrupt 180° turns at stops. It's a pretty "dumb" step - snap stops
    to nodes, and find the cheapest route between those nodes. Extra behavior can be
    obtained by subclassing this task and `injecting <https://en.wikipedia.org/wiki/Dependency_injection>`_
    extra behavior by overriding the ``create_xxx`` methods. See the documentation for
    :py:class:`AbstractGenerateShapes` for more details.
    """

    osm_resource: str
    """
    Name of the resource containing the `OSM XML <https://wiki.openstreetmap.org/wiki/OSM_XML>`_
    or `OSM PBF <https://wiki.openstreetmap.org/wiki/PBF_Format>`_ routing graph.
    """

    osm_profile: InitVar[routx.OsmProfile | routx.OsmCustomProfile | None] = None
    """
    Routing profile, which describes how OSM data should be converted to a routing graph.
    See documentation for `routx.OsmProfile <https://pypi.org/project/routx/#user-content-routxosmprofile>`_
    and `routx.OsmCustomProfile <https://pypi.org/project/routx/#user-content-routxosmcustomprofile>`_.

    Can be omitted (set to ``None``) only when the :py:attr:`route selector <routes>` explicitly
    states a :py:attr:`tram <impuls.model.Route.Type.TRAM>`,
    :py:attr:`metro <impuls.model.Route.Type.METRO>`,
    :py:attr:`rail <impuls.model.Route.Type.RAIL>`, or :py:attr:`bus <impuls.model.Route.Type.BUS>`
    route type. In this case, a corresponding
    `predefined profile <https://pypi.org/project/routx/#user-content-routxosmprofile>`_ is used.
    """

    osm_profile_resolved: routx.OsmProfile | routx.OsmCustomProfile = field(init=False)
    """
    This is not a parameter of the task; rather it's computed in
    `post-init processing <https://docs.python.org/3/library/dataclasses.html#post-init-processing>`_
    based on the value of :py:attr:`osm_profile` and :py:attr:`routes`.

    Resolved routing profile for `routx <https://pypi.org/project/routx/>`_.
    """

    osm_bbox: tuple[float, float, float, float] = (0.0, 0.0, 0.0, 0.0)
    """
    Bounding box for OpenStreetMap data. Any features outside of this box are ignored.
    Useful when loading a big OSM extract to route for a smaller area, to reduce memory usage.

    In order: left (min lon), bottom (min lat), right (max lon), top (max lat).

    Ignored if all values are 0, or one value is not finite.
    """

    max_stop_snap_distance_m: float = 500.0
    """
    Maximum allowed distance from a stop to its snapped node.

    When this distance is exceeded, the entire shape is dropped.
    """

    simplify_epsilon: float = 1e-5
    """
    Threshold for shape simplification using the
    `Ramer-Douglas-Peucker algorithm <https://en.wikipedia.org/wiki/Ramer%E2%80%93Douglas%E2%80%93Peucker_algorithm>`_,
    in decimal degrees.

    Set to ``nan`` to disable.
    """

    def __post_init__(
        self,
        task_name: str | None,
        osm_profile: routx.OsmProfile | routx.OsmCustomProfile | None,
    ) -> None:
        super().__post_init__(task_name)
        self.osm_profile_resolved = self.resolve_osm_profile(self.routes, osm_profile)

    def get_routing_graph(self, r: TaskRuntime) -> routx.Graph:
        g = routx.Graph()
        g.add_from_osm_file(
            r.resources[self.osm_resource].stored_at,
            self.osm_profile_resolved,
            bbox=self.osm_bbox,
        )
        return g

    def create_stop_snapper(self, r: TaskRuntime, graph: routx.Graph) -> StopSnapper:
        return (
            RoutxKDTreeStopSnapper(graph)
            .distance_limited(
                lambda node_id: ((n := graph[node_id]).lat, n.lon),
                self.max_stop_snap_distance_m,
            )
            .cached()
        )

    def create_leg_router(self, r: TaskRuntime, graph: routx.Graph) -> LegRouter:
        return RoutxLegRouter(graph, self.simplify_epsilon)

    @staticmethod
    def resolve_osm_profile(
        routes: selector.Routes,
        profile: routx.OsmProfile | routx.OsmCustomProfile | None,
    ) -> routx.OsmProfile | routx.OsmCustomProfile:
        if profile is not None:
            return profile

        elif routes.type is Route.Type.TRAM:
            return routx.OsmProfile.TRAM

        elif routes.type is Route.Type.METRO:
            return routx.OsmProfile.SUBWAY

        elif routes.type is Route.Type.RAIL:
            return routx.OsmProfile.RAILWAY

        elif routes.type is Route.Type.BUS:
            return routx.OsmProfile.BUS

        else:
            raise ValueError("missing osm_profile in GenerateShapes options")
