# © Copyright 2022-2024 Mikołaj Kuranowski
# SPDX-License-Identifier: GPL-3.0-or-later

import csv
import json
import shutil
import subprocess
from pathlib import Path
from typing import Generator, Mapping

from ..db import DBConnection
from ..model import Calendar, Date, Route, Stop, StopTime, TimePoint, Trip
from ..task import Task, TaskRuntime


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
    """LoadBusManMDB loads data into the database from a BusMan MDB database.

    Only the following entities are loaded: :py:class:`~impuls.model.Route`,
    :py:class:`~impuls.model.Stop`, :py:class:`~impuls.model.Calendar`,
    :py:class:`~impuls.model.Trip` and :py:class:`~impuls.model.StopTime`.

    :py:class:`~impuls.model.Agency` has to be manually curated beforehand (e.g. with
    :py:class:`~impuls.tasks.AddEntity` task).

    The imported :py:class:`~impuls.model.Calendar` entities will be empty after the import.
    Providing dates for calendars must be done manually afterwards.

    Most MDB databases seen in the wild have no stop positions. This step will
    set the latitude and longitude to 0. Further curation is usually necessary.

    Parameters:

    * ``resource``: name of the resource with MDB file
    * ``agency_id``: ID of the manually curated Agency
    * ``ignore_route_id``: use route_short_name as the ID, instead of the BusMan internal ID
    * ``ignore_stop_id``: use stop_code as the ID, instead of the BusMan internal ID
    * ``save_blocks``: use the tTeams table to fill block_id and block_short_name

    This task additionally requires `mdbtools <https://github.com/mdbtools/mdbtools>`_
    to be installed. This package is available in most package managers.
    """

    resource: str
    agency_id: str
    ignore_route_id: bool
    ignore_stop_id: bool
    save_blocks: bool

    _route_id_map: dict[str, str]
    _stop_id_map: dict[str, str]

    def __init__(
        self,
        resource: str,
        agency_id: str,
        ignore_route_id: bool = False,
        ignore_stop_id: bool = False,
        save_blocks: bool = False,
    ) -> None:
        super().__init__()
        self.resource = resource
        self.agency_id = agency_id
        self.ignore_route_id = ignore_route_id
        self.ignore_stop_id = ignore_stop_id
        self.save_blocks = save_blocks

        self._route_id_map = {}
        self._stop_id_map = {}

    def execute(self, r: TaskRuntime) -> None:
        self._route_id_map.clear()
        self._stop_id_map.clear()
        mdb_path = r.resources[self.resource].stored_at

        # Brief description on the BusMan MDB format
        # | Table Name | Impuls equiv. entity | Comments |
        # |------------|----------------------|----------|
        # | tLines     | Route                |
        # | tDirs      | -                    | Pattern
        # | tStakes    | Stop                 |
        # | tRoutes    | -                    | Pattern Stop
        # | tDayTypes  | Calendar             | (no dates)
        # | tTeams     | -                    | brigade (set of trips operated by one vehicle)
        # | tDepts     | Trip                 | Only departure from 1st stop
        # | tPassages  | StopTime             |
        # | tDays      | CalendarException    | (usually empty/useless)

        with r.db.transaction():
            self.logger.info("Loading routes")
            self.load_routes(mdb_path, r.db)

            self.logger.info("Loading calendars")
            self.load_calendars(mdb_path, r.db)

            self.logger.info("Loading stops")
            self.load_stops(mdb_path, r.db)

            self.logger.info("Loading trips")
            self.load_trips(mdb_path, r.db)

            self.logger.info("Loading stop times")
            self.load_stop_times(mdb_path, r.db)

    def load_routes(self, mdb_path: Path, db: DBConnection) -> None:
        # Fix for duplicate routes when using ignore_route_id
        seen_numbers: set[str] = set()

        for row in dump_mdb_table(mdb_path, "tLines"):
            # coalesce the route_id
            if self.ignore_route_id:
                route_id = row["nNumber"]
                self._route_id_map[row["ID"]] = route_id

                if route_id in seen_numbers:
                    continue
                else:
                    seen_numbers.add(route_id)

            else:
                route_id = row["ID"]

            # Create the new route
            db.create(
                Route(
                    id=route_id,
                    agency_id=self.agency_id,
                    short_name=row["nNumber"],
                    long_name=row["nName"],
                    type=Route.Type.BUS,
                )
            )

    def load_calendars(self, mdb_path: Path, db: DBConnection) -> None:
        for row in dump_mdb_table(mdb_path, "tDayTypes"):
            db.create(
                Calendar(
                    id=row["ID"],
                    monday=False,
                    tuesday=False,
                    wednesday=False,
                    thursday=False,
                    friday=False,
                    saturday=False,
                    sunday=False,
                    start_date=Date.SIGNALS_EXCEPTIONS,
                    end_date=Date.SIGNALS_EXCEPTIONS,
                    desc=row["nName"].strip(),
                )
            )

    def load_stops(self, mdb_path: Path, db: DBConnection) -> None:
        # Fix for duplicate stops when using ignore_stop_id
        seen_symbols: set[str] = set()

        for row in dump_mdb_table(mdb_path, "tStakes"):
            # Coalesce the stop_id
            if self.ignore_stop_id:
                stop_id = row["nSymbol"]
                self._stop_id_map[row["ID"]] = stop_id

                if stop_id in seen_symbols:
                    continue
                else:
                    seen_symbols.add(stop_id)

            else:
                stop_id = row["ID"]

            # Create the new stop
            db.create(
                Stop(
                    id=stop_id,
                    name=row["nName"],
                    lat=float(row["nLat"]) if row["nLat"] else 0.0,
                    lon=float(row["nLong"]) if row["nLong"] else 0.0,
                    code=row["nSymbol"],
                )
            )

    def load_trips(self, mdb_path: Path, db: DBConnection) -> None:
        pattern_to_route_id = {
            row["ID"]: self._route_id_map.get(row["nLine"], row["nLine"])
            for row in dump_mdb_table(mdb_path, "tDirs")
        }
        block_names = (
            {row["ID"]: row["nName"] for row in dump_mdb_table(mdb_path, "tTeams")}
            if self.save_blocks
            else {}
        )

        db.create_many(
            Trip,
            (
                Trip(
                    id=row["ID"],
                    route_id=pattern_to_route_id[row["nDir"]],
                    calendar_id=row["nDayType"],
                    block_id=row["nTeam"] if self.save_blocks else "",
                    extra_fields_json=(
                        json.dumps({"block_short_name": block_names.get(row["nTeam"], "")})
                        if self.save_blocks
                        else None
                    ),
                )
                for row in dump_mdb_table(mdb_path, "tDepts")
            ),
        )

    def load_stop_times(self, mdb_path: Path, db: DBConnection) -> None:
        db.create_many(
            StopTime,
            (
                StopTime(
                    trip_id=row["nDept"],
                    stop_id=self._stop_id_map.get(row["nStake"], row["nStake"]),
                    stop_sequence=int(row["nOrder"]),
                    arrival_time=TimePoint(seconds=int(row["nTime"]) * 60),
                    departure_time=TimePoint(seconds=int(row["nTime"]) * 60),
                )
                for row in dump_mdb_table(mdb_path, "tPassages")
            ),
        )
