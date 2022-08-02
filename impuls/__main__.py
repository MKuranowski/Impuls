from logging import Logger, getLogger
from typing import ClassVar

from .pipeline import Pipeline, PipelineOptions
from .db import DBConnection
from .tools.logs import initialize as initialize_logs


class DummyTask:
    name: ClassVar[str] = "DummyTask"
    logger: ClassVar[Logger] = getLogger("DummyTask")

    def execute(self, db: DBConnection, options: PipelineOptions) -> None:
        self.logger.info("Hello, world!")


initialize_logs(verbose=True)

p = Pipeline(
    tasks=[DummyTask()],
    options=PipelineOptions(),
)

p.run()
