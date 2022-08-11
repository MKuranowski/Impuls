from datetime import datetime
import logging
from zipfile import ZipFile
from io import TextIOWrapper
from csv import DictReader as CSVDictReader

from ...db import DBConnection
from ...pipeline import PipelineOptions, Task
from ...resource import Resource, ensure_resource_downloaded
from ... import model


class LoadGTFS(Task):
    source: Resource
    fetch_time: datetime | None

    name: str
    logger: logging.Logger

    def __init__(self, source: Resource) -> None:
        self.source = source
        self.fetch_time = None

        self.name = "LoadGTFS"
        self.logger = logging.getLogger(self.name)

    def execute(self, db: DBConnection, options: PipelineOptions) -> None:
        self.logger.info(f"Downloading the input GTFS file ({self.source.name})")
        gtfs_path = ensure_resource_downloaded(
            self.source,
            options.workspace_directory,
            options.ignore_not_modified,
        )

        # Try to import every table
        # NOTE: neither calendar nor calendar_dates are required,
        #       but missing calendars will trigger foreign key violations on import
        required_tables = {"routes", "stops", "trips", "stop_times"}

        with ZipFile(gtfs_path, mode="r") as arch:
            for typ in model.ALL_MODEL_ENTITIES:
                # Find the table file
                try:
                    arch.getinfo(typ._gtfs_table_name + ".txt")
                except KeyError:
                    if typ._gtfs_table_name in required_tables:
                        self.logger.fatal(f"Missing GTFS table: {typ._gtfs_table_name}")
                        raise
                    else:
                        self.logger.info(f"Missing GTFS table: {typ._gtfs_table_name}")
                        continue

                # Read the table
                with arch.open(typ._gtfs_table_name + ".txt") as bytes_csv_buffer:
                    self.logger.info(f"Loading table: {typ._gtfs_table_name}")
                    reader = CSVDictReader(
                        TextIOWrapper(bytes_csv_buffer, encoding="utf-8-sig", newline="")
                    )

                    for row in reader:
                        # Hacks to conform the GTFS model to Impuls model

                        # agency_id is required by Impuls, but not GTFS
                        if typ in (model.Agency, model.Route) and not row.get("agency_id"):
                            row["agency_id"] = "(missing)"

                        # CalendarException can exist without parent Calendar in GTFS,
                        # but not Impuls
                        if (
                            typ is model.CalendarException
                            and db.retrieve(model.Calendar, row["service_id"]) is None
                        ):
                            db.save(
                                model.Calendar(
                                    row["service_id"],
                                    monday=False,
                                    tuesday=False,
                                    wednesday=False,
                                    thursday=False,
                                    friday=False,
                                    saturday=False,
                                    sunday=False,
                                    start_date=model.Date.SIGNALS_EXCEPTIONS,
                                    end_date=model.Date.SIGNALS_EXCEPTIONS,
                                )
                            )

                        # Attribution has to have an id
                        if typ is model.Attribution and not row.get("attribution_id"):
                            row["attribution_id"] = str(reader.line_num)

                        # Persist the entity
                        db.save(typ._gtfs_unmarshall(row))
