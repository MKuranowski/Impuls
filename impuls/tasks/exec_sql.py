import logging

from .. import Task, TaskRuntime


class ExecuteSQL(Task):
    """ExecuteSQL task simply executes the provided statement."""

    def __init__(self, name: str, statement: str) -> None:
        self.name = name
        self.statement = statement

        self.logger = logging.getLogger(self.name)

    def execute(self, r: TaskRuntime) -> None:
        r.db.raw_execute(self.statement)
