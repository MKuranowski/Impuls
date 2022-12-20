import logging

from .. import DBConnection, PipelineOptions, ResourceManager, Task


class GenerateTripHeadsign(Task):
    """GenerateTripHeadsign is a task which fills the trip_headsign
    field for all trips which don't already have a headsign.

    The generated headsign is the name of the last stop of the trip.
    This step will break if there are trips without any stops."""

    def __init__(self) -> None:
        self.name = type(self).__name__
        self.logger = logging.getLogger(self.name)

    def execute(
        self,
        db: DBConnection,
        options: PipelineOptions,
        resources: ResourceManager,
    ) -> None:
        # mmm yes, nice correlated nested select statement :)
        # will break on trips with no stop times
        db.raw_execute(
            """
            UPDATE trips SET headsign = (
                SELECT s.name FROM stop_times AS st
                LEFT JOIN stops AS s ON (st.stop_id = s.stop_id)
                WHERE st.trip_id = trips.trip_id
                ORDER BY st.stop_sequence DESC
                LIMIT 1
            )
            WHERE headsign ISNULL OR headsign = '';
            """
        )
