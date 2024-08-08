# © Copyright 2022-2024 Mikołaj Kuranowski
# SPDX-License-Identifier: GPL-3.0-or-later

from tempfile import TemporaryDirectory
from zipfile import ZipFile

from .. import extern
from ..task import Task, TaskRuntime
from ..tools.types import StrPath

ALLOWED_FILES = {
    "agency.txt",
    "attributions.txt",
    "calendar.txt",
    "calendar_dates.txt",
    "feed_info.txt",
    "routes.txt",
    "stops.txt",
    "fare_attributes.txt",
    "fare_rules.txt",
    "shapes.txt",
    "trips.txt",
    "stop_times.txt",
    "frequencies.txt",
    "transfers.txt",
    "translations.txt",
}


class LoadGTFS(Task):
    """LoadGTFS attempts to load GTFS data from a ZIP archive.

    The loader only supports a subset of the GTFS schema. Due to implementation details, some
    invalid values may be accepted and some valid values may be rejected. In particular:

    * stops.txt location_types 3, 4, and 5 will cause an error,
    * parent_station may only refer to stop_ids defined in earlier lines,
    * agency_id in fare_attributes.txt is required if it's present in agency.txt,
      even if there's only one agency defined in the dataset.
    """

    resource: str

    def __init__(self, resource: str) -> None:
        super().__init__()
        self.resource = resource

    def execute(self, r: TaskRuntime) -> None:
        gtfs_path = r.resources[self.resource].stored_at
        with TemporaryDirectory(prefix="impuls-load-gtfs-") as gtfs_dir:
            self.logger.info("Extracting %s", self.resource)
            self.extract_gtfs_to(gtfs_path, gtfs_dir)

            self.logger.info("Loading %s", self.resource)
            with r.db.released() as db_path:
                extern.load_gtfs(db_path, gtfs_dir)

    def extract_gtfs_to(self, zip_path: StrPath, dir_path: StrPath) -> None:
        with ZipFile(zip_path, mode="r") as zip:
            for zip_file in zip.infolist():
                if zip_file.filename not in ALLOWED_FILES:
                    continue

                self.logger.debug("Extracting %s", zip_file.filename)
                zip.extract(zip_file, dir_path)
