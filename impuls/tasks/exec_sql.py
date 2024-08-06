from ..task import Task, TaskRuntime


class ExecuteSQL(Task):
    """ExecuteSQL task simply executes the provided statement."""

    statement: str

    def __init__(self, task_name: str, statement: str) -> None:
        super().__init__(name=task_name)
        self.statement = statement

    def execute(self, r: TaskRuntime) -> None:
        r.db.raw_execute(self.statement)
