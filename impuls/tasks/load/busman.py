import csv
import logging
import shutil
import subprocess
from pathlib import Path
from typing import Generator, Mapping

from ... import DBConnection, PipelineOptions, ResourceManager, Task, model


def dump_mdb_table(database: Path, table_name: str) -> Generator[Mapping[str, str], None, None]:
    """Dumps rows from a specific table of an MDB database,
    using an external CLI tool called "mdb-export" (from mdbtools).
    """
    # Try to find the mdb-export program
    mdb_export_path = shutil.which("mdb-export")
    if mdb_export_path is None:
        raise RuntimeError("mdb-export couldn't be found! Do you have mdbtools installed?")

    # Launch the program, and ensure we are hooked up to the stdout
    process = subprocess.Popen(
        [mdb_export_path, str(database), table_name],
        stdout=subprocess.PIPE,
        encoding="utf-8",  # Maybe we should use sys.stdin.encoding?
        errors="raise",
    )

    # Generate the rows and wait for mdb-export to finish
    try:
        assert process.stdout is not None
        yield from csv.DictReader(process.stdout)
    finally:
        return_code = process.wait()

    # Check that the program has succeeded
    if return_code != 0:
        raise RuntimeError(
            f"mdb-export {database.name!r} {table_name!r} failed with return code {return_code}",
        )


class LoadBusManMDB(Task):
    """LoadBusManMDB loads data into the database from a
    BusMan MDB database.

    Only the following entities are loaded:
    Lines, Stops, Calendars, Trips, StopTimes.

    Agency has to be manually curated beforehand.
    Calendar or CalendarExceptions have to be manually curated after the import.

    Parameters:
    - `source`: the MDB file resource
    - `agency_id`: ID of the manually curated Agency
    - `ignore_route_id`: use route_short_name as the ID,
        instead of the BusMan internal ID
    - `ignore_stop_id`: use stop_code as the ID,
        instead of the BusMan internal ID
    """

    source: str
    agency_id: str
    ignore_route_id: bool
    ignore_stop_id: bool

    _route_id_map: dict[str, str]
    _stop_id_map: dict[str, str]

    name: str
    logger: logging.Logger

    def __init__(
        self,
        source: str,
        agency_id: str,
        ignore_route_id: bool = False,
        ignore_stop_id: bool = False,
    ) -> None:
        self.source = source
        self.agency_id = agency_id
        self.ignore_route_id = ignore_route_id
        self.ignore_stop_id = ignore_stop_id
        self.fetch_time = None

        self._route_id_map = {}
        self._stop_id_map = {}

        self.name = "LoadBusManMDB"
        self.logger = logging.getLogger(f"Task.{self.name}")

    def execute(
        self, db: DBConnection, options: PipelineOptions, resources: ResourceManager
    ) -> None:
        self._route_id_map.clear()
        self._stop_id_map.clear()
        mdb_path = resources.get_resource_path(self.source)

        # Brief description on the BusMan MDB format
        # | Table Name | Impuls equiv. entity | Comments |
        # |------------|----------------------|----------|
        # | tLines     | Line                 |
        # | tDirs      | -                    | Pattern
        # | tStakes    | Stop                 |
        # | tRoutes    | -                    | Pattern Stop
        # | tDayTypes  | Calendar             | (no dates)
        # | tTeams     | -                    | brigade (set of trips operated by one vehicle)
        # | tDepts     | Trip                 | Only departure from 1st stop
        # | tPassages  | StopTime             |
        # | tDays      | CalendarException    | (usually empty/useless)

        self.logger.info("Loading routes")
        self.load_routes(mdb_path, db)

        self.logger.info("Loading calendars")
        self.load_calendars(mdb_path, db)

        self.logger.info("Loading stops")
        self.load_stops(mdb_path, db)

        self.logger.info("Loading trips")
        self.load_trips(mdb_path, db)

        self.logger.info("Loading stop times")
        self.load_stop_times(mdb_path, db)

    def load_routes(self, mdb_path: Path, db: DBConnection) -> None:
        for row in dump_mdb_table(mdb_path, "tLines"):
            # coalesce the route_id
            if self.ignore_route_id:
                route_id = row["nNumber"]
                self._route_id_map[row["ID"]] = route_id
            else:
                route_id = row["ID"]

            # Create the new route
            db.save(
                model.Route(
                    id=route_id,
                    agency_id=self.agency_id,
                    short_name=row["nNumber"],
                    long_name=row["nName"],
                    type=model.Route.Type.BUS,
                )
            )

    def load_calendars(self, mdb_path: Path, db: DBConnection) -> None:
        for row in dump_mdb_table(mdb_path, "tDayTypes"):
            db.save(
                model.Calendar(
                    id=row["ID"],
                    monday=False,
                    tuesday=False,
                    wednesday=False,
                    thursday=False,
                    friday=False,
                    saturday=False,
                    sunday=False,
                    start_date=model.Date.SIGNALS_EXCEPTIONS,
                    end_date=model.Date.SIGNALS_EXCEPTIONS,
                    desc=row["nName"],
                )
            )

    def load_stops(self, mdb_path: Path, db: DBConnection) -> None:
        for row in dump_mdb_table(mdb_path, "tStakes"):
            # Coalesce the stop_id
            if self.ignore_stop_id:
                stop_id = row["nSymbol"].rstrip("_").replace("-", "")
                self._stop_id_map[row["ID"]] = stop_id
            else:
                stop_id = row["ID"]

            # Create the new stop
            db.save(
                model.Stop(
                    id=stop_id,
                    name=row["nName"],
                    lat=float(row["nLat"]) if row["nLat"] else 0.0,
                    lon=float(row["nLong"]) if row["nLong"] else 0.0,
                    code=row["nSymbol"].rstrip("_").replace("-", ""),
                )
            )

    def load_trips(self, mdb_path: Path, db: DBConnection) -> None:
        pattern_to_route_id = {
            row["ID"]: self._route_id_map.get(row["nLine"], row["nLine"])
            for row in dump_mdb_table(mdb_path, "tDirs")
        }

        for row in dump_mdb_table(mdb_path, "tDepts"):
            db.save(
                model.Trip(
                    id=row["ID"],
                    route_id=pattern_to_route_id[row["nDir"]],
                    calendar_id=row["nDayType"],
                )
            )

    def load_stop_times(self, mdb_path: Path, db: DBConnection) -> None:
        for row in dump_mdb_table(mdb_path, "tPassages"):
            time = model.TimePoint(seconds=int(row["nTime"]) * 60)
            db.save(
                model.StopTime(
                    trip_id=row["nDept"],
                    stop_id=self._stop_id_map.get(row["nStake"], row["nStake"]),
                    stop_sequence=int(row["nOrder"]),
                    arrival_time=time,
                    departure_time=time,
                )
            )
