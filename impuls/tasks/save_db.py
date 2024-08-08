# © Copyright 2022-2024 Mikołaj Kuranowski
# SPDX-License-Identifier: GPL-3.0-or-later

import sqlite3
from pathlib import Path

from ..task import Task, TaskRuntime
from ..tools.types import StrPath


class SaveDB(Task):
    """SaveDB saves the contained data as-is to a database at a provided path."""

    to: Path

    def __init__(self, to: StrPath) -> None:
        super().__init__()
        self.to = Path(to)

    def execute(self, r: TaskRuntime) -> None:
        with sqlite3.connect(self.to) as target:
            r.db._con.backup(target)
