import logging

from .. import model
from ..db import DBConnection
from ..pipeline import Task, PipelineOptions
from ..resource import ResourceManager


class AddEntity(Task):
    """AddEntity is a simple task that adds the provided entity to the DB."""

    entity: model.ImpulsBase
    name: str
    logger: logging.Logger

    def __init__(self, entity: model.ImpulsBase) -> None:
        self.entity = entity
        self.name = "AddAgency"
        self.logger = logging.getLogger(f"Task.{self.name}")

    def execute(
        self, db: DBConnection, options: PipelineOptions, resources: ResourceManager
    ) -> None:
        db.save(self.entity)
