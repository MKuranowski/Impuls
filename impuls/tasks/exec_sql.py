from typing import final

from .. import Task, TaskRuntime


@final
class ExecuteSQL(Task):
    """ExecuteSQL task simply executes the provided statement."""

    def __init__(self, task_name: str, statement: str) -> None:
        super().__init__(name=task_name)
        self.statement = statement

    def execute(self, r: TaskRuntime) -> None:
        r.db.raw_execute(self.statement)
