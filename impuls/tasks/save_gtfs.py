import csv
from io import TextIOWrapper
from pathlib import Path
from typing import IO, Mapping, Sequence, Type, final
from zipfile import ZipFile

from ..model import ALL_MODEL_ENTITIES, Calendar, Entity
from ..task import DBConnection, Task, TaskRuntime

GTFSHeaders = Mapping[str, Sequence[str]]

MODEL_TYPE_BY_GTFS_FILE_NAME = {t.gtfs_table_name(): t for t in ALL_MODEL_ENTITIES}


@final
class SaveGTFS(Task):
    """SaveGTFS exports the contained data to as a GTFS zip file at the provided path.

    `headers` is a mapping from a GTFS table (excluding the .txt extension) to a sequence of
    colum names. SaveGTFS doesn't validate the provided mapping, so the caller must ensure
    all required columns and files are provided.

    When `emit_empty_calendars` is set to True (default is False), empty calendars will
    still be generated in the calendar.txt file.
    """

    def __init__(
        self,
        headers: GTFSHeaders,
        target: Path,
        emit_empty_calendars: bool = False,
    ) -> None:
        super().__init__()
        self.headers = headers
        self.target = target
        self.emit_empty_calendars = emit_empty_calendars

    def execute(self, r: TaskRuntime) -> None:
        self.logger.info("Opening %s", self.target)
        with ZipFile(self.target, mode="w") as archive:
            for table_name, fields in self.headers:
                self.logger.info("Writing %s", table_name)
                typ = MODEL_TYPE_BY_GTFS_FILE_NAME[table_name]

                with archive.open(f"{table_name}.txt", mode="w") as file:
                    buffer = TextIOWrapper(file, "utf-8", newline="")
                    if typ is Calendar:
                        self.dump_calendars(r.db, buffer, fields)
                    else:
                        self.dump_table(r.db, typ, buffer, fields)

    @staticmethod
    def dump_table(
        db: DBConnection,
        typ: Type[Entity],
        to: IO[str],
        fields: Sequence[str],
    ) -> None:
        w = csv.DictWriter(to, fields, extrasaction="ignore")
        w.writeheader()
        for obj in db.retrieve_all(typ):
            w.writerow(obj.gtfs_marshall())

    def dump_calendars(self, db: DBConnection, to: IO[str], fields: Sequence[str]) -> None:
        w = csv.DictWriter(to, fields, extrasaction="ignore")
        w.writeheader()
        for obj in db.retrieve_all(Calendar):
            if not self.emit_empty_calendars and not obj.compute_active_dates():
                continue

            w.writerow(obj.gtfs_marshall())
