import sqlite3
from pathlib import Path
from typing import final

from ..task import Task, TaskRuntime


@final
class SaveDB(Task):
    """SaveDB saves the contained data as-is to a database at a provided path"""

    def __init__(self, to: Path) -> None:
        super().__init__()
        self.to = to

    def execute(self, r: TaskRuntime) -> None:
        with sqlite3.connect(self.to) as target:
            r.db._con.backup(target)
