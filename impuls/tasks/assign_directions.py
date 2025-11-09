# © Copyright 2025 Mikołaj Kuranowski
# SPDX-License-Identifier: GPL-3.0-or-later

from typing import Iterable, Literal, cast

from .. import selector
from ..db import DBConnection
from ..errors import DataError, MultipleDataErrors
from ..task import Task, TaskRuntime


class AssignDirections(Task):
    """AssignDirections sets :py:attr:`Trip.direction <impuls.model.Trip.direction>`
    based on a list of stop pairs defining the outbound direction.
    """

    outbound_stop_pairs: list[tuple[str, str]]
    """Stop ID pairs defining the :py:const:`outbound <impuls.model.Trip.Direction.OUTBOUND>`
    direction.

    If a trip stops at the first stop of a pair before stopping at the second stop,
    it will be assigned to the outbound direction. Otherwise, if a trip stops at the
    second stop before stopping at the first stop, it will be assigned the inbound direction.

    Only unambiguous stops (where the trip calls exactly once) are considered.

    All selected trips must match at least one pair, otherwise this step fails with
    a :py:class:`~impuls.errors.MultipleDataErrors`.

    This might be easier explained by the algorithm's pseudo code::

        def assign_direction_id(trip_stop_id_to_stop_sequence: Mapping[str, int]) -> Direction:
            for stop_a, stop_b in outbound_stop_pairs:
                idx_a = trip_stop_id_to_stop_sequence.get(stop_a)
                idx_b = trip_stop_id_to_stop_sequence.get(stop_b)
                if idx_a is not None and idx_b is not None:
                    return Direction.OUTBOUND if idx_a < idx_b else Direction.INBOUND
            raise DataError(...)
    """

    routes: selector.Routes
    """Selects routes for which direction assignment should run. Defaults to all routes."""

    overwrite: bool
    """Should the step overwrite existing directions? Defaults to ``False``, preserving
    any existing :py:attr:`Trip.direction <impuls.model.Trip.direction>`.
    """

    def __init__(
        self,
        outbound_stop_pairs: Iterable[tuple[str, str]],
        routes: selector.Routes = selector.Routes(),
        overwrite: bool = False,
        task_name: str | None = None,
    ) -> None:
        super().__init__(name=task_name)
        self.outbound_stop_pairs = list(outbound_stop_pairs)
        self.routes = routes
        self.overwrite = overwrite

    def execute(self, r: TaskRuntime) -> None:
        self.logger.debug("Finding trips to process")
        trip_ids = self.get_trip_ids_to_process(r.db)

        self.logger.debug(
            "Processing %d trip%s",
            len(trip_ids),
            "s" if len(trip_ids) != 1 else "",
        )
        assigned_directions = MultipleDataErrors.catch_all(
            "direction assignment",
            map(lambda trip_id: self.find_direction_of_trip(r.db, trip_id), trip_ids),
        )

        with r.db.transaction():
            r.db.raw_execute_many(
                "UPDATE trips SET direction = ? WHERE trip_id = ?",
                assigned_directions,
            )
        self.logger.info(
            "Assigned direction to %d trip%s",
            len(trip_ids),
            "s" if len(trip_ids) != 1 else "",
        )

    def get_trip_ids_to_process(self, db: DBConnection) -> list[str]:
        """Returns a list of all trip_ids for which this step should run.

        This includes all trips belonging to the routes selected by the configured
        :py:attr:`selector <impuls.tasks.AssignDirections.routes>`, without any direction
        (unless :py:attr:`overwrite <impuls.tasks.AssignDirections.overwrite` is set).
        """
        route_ids = list(self.routes.find_ids(db))
        query = "SELECT trip_id FROM trips WHERE route_id = ?"
        if not self.overwrite:
            query += " AND direction IS NULL"
        return [
            cast(str, i[0]) for route_id in route_ids for i in db.raw_execute(query, (route_id,))
        ]

    def find_direction_of_trip(self, db: DBConnection, trip_id: str) -> tuple[Literal[0, 1], str]:
        """Attempts to assign a direction to the provided trip.

        Returns a tuple of the assigned direction and the trip_id, so that the returned
        tuple can be directly used in an UPDATE statement.

        If the direction can't be assigned, raises :py:class:`~impuls.errors.DataError`.
        """
        sequence_by_stop = self.get_unambiguous_stop_sequences(db, trip_id)
        for stop_a, stop_b in self.outbound_stop_pairs:
            idx_a = sequence_by_stop.get(stop_a)
            idx_b = sequence_by_stop.get(stop_b)
            if idx_a is not None and idx_b is not None:
                return (0 if idx_a < idx_b else 1), trip_id
        raise DataError(f"no direction for trip {trip_id}")

    def get_unambiguous_stop_sequences(self, db: DBConnection, trip_id: str) -> dict[str, int]:
        """Returns a mapping of stop_id to stop_sequence for the provided trip,
        excluding any ambiguous stops - stops where the trip calls at more than once.
        """
        return {
            cast(str, i[0]): cast(int, i[1])
            for i in db.raw_execute(
                "SELECT stop_id, stop_sequence FROM stop_times WHERE trip_id = ? "
                "GROUP BY stop_id HAVING COUNT(*) = 1",
                (trip_id,),
            )
        }
