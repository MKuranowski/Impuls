from typing import final

from .. import Task, TaskRuntime, model


@final
class AddEntity(Task):
    """AddEntity is a simple task that adds the provided entity to the DB."""

    entity: model.Entity

    def __init__(self, entity: model.Entity, task_name: str = "AddEntity") -> None:
        super().__init__(name=task_name)
        self.entity = entity

    def execute(self, r: TaskRuntime) -> None:
        r.db.create(self.entity)
