from csv import DictReader as CSVDictReader
from datetime import datetime
from io import TextIOWrapper
from typing import final
from zipfile import ZipFile

from ..model import (
    ALL_MODEL_ENTITIES,
    Agency,
    Attribution,
    Calendar,
    CalendarException,
    Date,
    FareAttribute,
    Route,
    ShapePoint,
)
from ..task import Task, TaskRuntime


@final
class LoadGTFS(Task):
    def __init__(self, resource: str) -> None:
        super().__init__()
        self.resource = resource

    def execute(self, r: TaskRuntime) -> None:
        gtfs_path = r.resources[self.resource].stored_at
        self.fetch_time = datetime.today()

        # Try to import every table
        # NOTE: neither calendar nor calendar_dates are required,
        #       but missing calendars will trigger foreign key violations on import
        required_tables = {"routes", "stops", "trips", "stop_times"}

        # If there was only one agency_id, remember its ID
        # This is required for loading fare_attributes, where agency_id can be ommited
        # if there is exactly one agency defined in agency.txt
        agency_ids: list[str] = []

        with ZipFile(gtfs_path, mode="r") as arch, r.db.transaction():
            for typ in ALL_MODEL_ENTITIES:
                # Find the table file
                try:
                    arch.getinfo(typ.gtfs_table_name() + ".txt")
                except KeyError:
                    if typ.gtfs_table_name() in required_tables:
                        self.logger.fatal(f"Missing GTFS table: {typ.gtfs_table_name()}")
                        raise
                    else:
                        self.logger.info(f"Missing GTFS table: {typ.gtfs_table_name()}")
                        continue

                # Read the table
                with arch.open(typ.gtfs_table_name() + ".txt") as bytes_csv_buffer:
                    self.logger.info(f"Loading table: {typ.gtfs_table_name()}")
                    reader = CSVDictReader(
                        TextIOWrapper(bytes_csv_buffer, encoding="utf-8-sig", newline="")
                    )

                    for row in reader:
                        # Hacks to conform the GTFS model to Impuls model

                        # agency_id is required by Impuls, but not GTFS
                        if typ in (Agency, Route) and not row.get("agency_id"):
                            row["agency_id"] = "(missing)"

                        # Remember & recall agency_ids, for loading fare_attributes
                        if typ is Agency:
                            agency_ids.append(row["agency_id"])
                        if typ is FareAttribute and not row.get("agency_id"):
                            if len(agency_ids) != 1:
                                raise ValueError(
                                    "fare_attribute has no agency_id, but there were "
                                    f"multiple agencies defined ({agency_ids})"
                                )
                            row["agency_id"] = agency_ids[0]

                        # CalendarException can exist without parent Calendar in GTFS,
                        # but not Impuls
                        if (
                            typ is CalendarException
                            and r.db.retrieve(Calendar, row["service_id"]) is None
                        ):
                            r.db.create(
                                Calendar(
                                    row["service_id"],
                                    monday=False,
                                    tuesday=False,
                                    wednesday=False,
                                    thursday=False,
                                    friday=False,
                                    saturday=False,
                                    sunday=False,
                                    start_date=Date.SIGNALS_EXCEPTIONS,
                                    end_date=Date.SIGNALS_EXCEPTIONS,
                                )
                            )

                        # shape_id is virtual in GTFS, but concrete in Impuls
                        if typ is ShapePoint:
                            r.db.raw_execute(
                                "INSERT OR IGNORE INTO shapes (shape_id) VALUES (?)",
                                (row["shape_id"],),
                            )

                        # Attribution has to have an id
                        if typ is Attribution and not row.get("attribution_id"):
                            row["attribution_id"] = str(reader.line_num)

                        # Persist the entity
                        r.db.create(typ.gtfs_unmarshall(row))
