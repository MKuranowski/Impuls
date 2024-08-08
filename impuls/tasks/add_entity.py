# © Copyright 2022-2024 Mikołaj Kuranowski
# SPDX-License-Identifier: GPL-3.0-or-later

from ..model import Entity
from ..task import Task, TaskRuntime


class AddEntity(Task):
    """AddEntity is a simple task that adds the provided entity to the DB."""

    entity: Entity

    def __init__(self, entity: Entity, task_name: str = "AddEntity") -> None:
        super().__init__(name=task_name)
        self.entity = entity

    def execute(self, r: TaskRuntime) -> None:
        r.db.create(self.entity)
