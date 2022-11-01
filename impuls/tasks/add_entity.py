import logging

from .. import DBConnection, PipelineOptions, ResourceManager, Task, model


class AddEntity(Task):
    """AddEntity is a simple task that adds the provided entity to the DB."""

    entity: model.ImpulsBase
    name: str
    logger: logging.Logger

    def __init__(self, entity: model.ImpulsBase, task_name: str = "AddEntity") -> None:
        self.entity = entity
        self.name = task_name
        self.logger = logging.getLogger(f"Task.{self.name}")

    def execute(
        self, db: DBConnection, options: PipelineOptions, resources: ResourceManager
    ) -> None:
        db.create(self.entity)
