# © Copyright 2022-2025 Mikołaj Kuranowski
# SPDX-License-Identifier: GPL-3.0-or-later

import os
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Mapping, Sequence
from zipfile import ZIP_DEFLATED, ZipFile

from .. import extern
from ..task import DBConnection, Task, TaskRuntime
from ..tools.types import StrPath

GTFSHeaders = Mapping[str, Sequence[str]]


class SaveGTFS(Task):
    """SaveGTFS exports the contained data to as a GTFS zip file at the provided path.

    ``headers`` is a mapping from a GTFS table to a sequence of column names.

    All keys must include the ``.txt`` extension. If a GTFS table exists both as a standalone
    SQL table and as a :py:class:`~impuls.model.ExtraTableRow`, the former will always
    be preferred. For example::

        headers = {
            "agency.txt": ("agency_id", "agency_name", "agency_url", "agency_timezone", "agency_lang"),
            "routes.txt": ("agency_id", "route_id", "route_short_name", "route_long_name", "route_type"),
            "extra_file.txt": ("custom_column_1", "custom_column_2"),
        }

    SaveGTFS doesn't validate the provided mapping, so the caller must ensure
    all required columns and files are provided.

    The table names are used as file names and must not contain any path separators,
    or other disallowed characters/character sequences by the current OS and file system.

    When ``emit_empty_calendars`` is set to True (default is False), empty calendars will
    still be generated in the calendar.txt file.

    When ``ensure_order`` is set to True (most) output tables will be sorted by their primary key.
    This might slow down the task. The default is False, which saves tables in arbitrary order.
    """  # noqa: E501

    headers: GTFSHeaders
    target: Path
    emit_empty_calendars: bool
    ensure_order: bool

    def __init__(
        self,
        headers: GTFSHeaders,
        target: StrPath,
        emit_empty_calendars: bool = False,
        ensure_order: bool = False,
    ) -> None:
        super().__init__()
        self.headers = headers
        self.target = Path(target)
        self.emit_empty_calendars = emit_empty_calendars
        self.ensure_order = ensure_order

    def execute(self, r: TaskRuntime) -> None:
        with TemporaryDirectory(prefix="impuls-save-gtfs-") as temp_dir:
            self.dump_tables(temp_dir, r.db)
            self.create_zip(temp_dir)

    def dump_tables(self, gtfs_dir: StrPath, db: DBConnection) -> None:
        self.logger.info("Dumping tables")
        with db.released() as db_path:
            extern.save_gtfs(
                db_path,
                gtfs_dir,
                self.headers,
                self.emit_empty_calendars,
                self.ensure_order,
            )

    def create_zip(self, dir: StrPath) -> None:
        self.logger.info("Compressing to %s", self.target)
        with ZipFile(self.target, mode="w", compression=ZIP_DEFLATED) as archive:
            for file_name in self.headers:
                self.logger.debug("Compressing %s", file_name)
                archive.write(os.path.join(dir, file_name), file_name)
