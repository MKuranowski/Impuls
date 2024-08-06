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

    ``headers`` is a mapping from a GTFS table (excluding the .txt extension) to a sequence of
    colum names. SaveGTFS doesn't validate the provided mapping, so the caller must ensure
    all required columns and files are provided.

    When ``emit_empty_calendars`` is set to True (default is False), empty calendars will
    still be generated in the calendar.txt file.
    """

    headers: GTFSHeaders
    target: Path
    emit_empty_calendars: bool

    def __init__(
        self,
        headers: GTFSHeaders,
        target: StrPath,
        emit_empty_calendars: bool = False,
    ) -> None:
        super().__init__()
        self.headers = headers
        self.target = Path(target)
        self.emit_empty_calendars = emit_empty_calendars

    def execute(self, r: TaskRuntime) -> None:
        with TemporaryDirectory(prefix="impuls-save-gtfs-") as temp_dir:
            self.dump_tables(temp_dir, r.db)
            self.create_zip(temp_dir)

    def dump_tables(self, gtfs_dir: StrPath, db: DBConnection) -> None:
        self.logger.info("Dumping tables")
        with db.released() as db_path:
            extern.save_gtfs(db_path, gtfs_dir, self.headers, self.emit_empty_calendars)

    def create_zip(self, dir: StrPath) -> None:
        self.logger.info("Compressing to %s", self.target)
        with ZipFile(self.target, mode="w", compression=ZIP_DEFLATED) as archive:
            for table_name in self.headers:
                file_name = table_name + ".txt"
                self.logger.debug("Compressing %s", file_name)
                archive.write(os.path.join(dir, file_name), file_name)
