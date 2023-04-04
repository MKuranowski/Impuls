import logging

from .. import Task, TaskRuntime, model


class AddEntity(Task):
    """AddEntity is a simple task that adds the provided entity to the DB."""

    entity: model.Entity
    name: str
    logger: logging.Logger

    def __init__(self, entity: model.Entity, task_name: str = "AddEntity") -> None:
        self.entity = entity
        self.name = task_name
        self.logger = logging.getLogger(f"Task.{self.name}")

    def execute(self, r: TaskRuntime) -> None:
        r.db.create(self.entity)
