import logging
from ..db import DBConnection
from ..pipeline import PipelineOptions, Task
from ..resource import ResourceManager


class ExecuteSQL(Task):
    """ExecuteSQL task simply executes the provided statement."""

    def __init__(self, name: str, statement: str) -> None:
        self.name = name
        self.statement = statement

        self.logger = logging.getLogger(self.name)

    def execute(
        self, db: DBConnection, options: PipelineOptions, resources: ResourceManager
    ) -> None:
        db.raw_execute(self.statement)
