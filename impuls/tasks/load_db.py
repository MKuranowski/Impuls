# © Copyright 2024 Mikołaj Kuranowski
# SPDX-License-Identifier: GPL-3.0-or-later

import sqlite3

from ..task import Task, TaskRuntime


class LoadDB(Task):
    """LoadDB overwrites the runtime database by data from a databases
    in the provided resource. The database must have been created by Impuls,
    usually by the :py:class:`~impuls.tasks.SaveDB` task or by the runtime as the
    ``impuls.db`` file in the :py:attr:`~impuls.PipelineOptions.workspace_directory`.
    Mismatched schemas will cause problems later down the line.
    """

    resource: str

    def __init__(self, resource: str) -> None:
        super().__init__()
        self.resource = resource

    def execute(self, r: TaskRuntime) -> None:
        with sqlite3.connect(r.resources[self.resource].stored_at) as source:
            source.backup(r.db._con)
