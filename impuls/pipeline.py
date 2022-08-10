import logging
from pathlib import Path
from typing import Any, NamedTuple, Protocol

from .db import DBConnection
from .tools import machine_load
from .tools.types import Self


class PipelineOptions(NamedTuple):
    # By default import tasks are supposed to raise InputNotModified
    # if their input resource has not changed.
    ignore_not_modified: bool = False

    # Directory where tasks can cache their workload to preserve it across
    # multiple runs. Intended to be used by import tasks to avoid
    # re-downloading of files that were retrieved in the past.
    #
    # If the given directory doesn't exists, Pipeline attempts
    # to create it (and its parents)
    workspace_directory: Path = Path("_impuls_workspace")

    # By default Impuls saves the sqlite DB in-memory.
    # Setting this flag to true causes the DB to be saved in the workspace
    # directory instead.
    save_db_in_workspace: bool = False


class Task(Protocol):
    name: str
    logger: logging.Logger

    def execute(self, db: DBConnection, options: PipelineOptions) -> None:
        ...


class Pipeline:
    def __init__(
        self,
        tasks: list[Task],
        options: PipelineOptions = PipelineOptions(),
        name: str = "",
    ) -> None:
        # Set parameters
        self.name: str = name
        self.logger: logging.Logger = logging.getLogger(f"{name}.Pipeline" if name else "Pipeline")
        self.tasks: list[Task] = tasks
        self.options: PipelineOptions = options

        # Update task loggers
        if self.name:
            for task in self.tasks:
                task.logger = self.logger.getChild(task.logger.name)

        # Ensure the workspace directory exists
        self.options.workspace_directory.mkdir(parents=True, exist_ok=True)

        # Open the database
        self.db: DBConnection = DBConnection.create_with_schema(
            str(self.options.workspace_directory / "impuls.db")
            if self.options.save_db_in_workspace
            else ":memory:"
        )

    def close(self) -> None:
        self.db.close()

    def __enter__(self: Self) -> Self:
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    def run(self) -> None:
        for task in self.tasks:
            self.logger.info(f"Executing task {task.name}")

            with machine_load.LoadTracker() as resource_usage:
                task.execute(self.db, self.options)

            self.logger.debug(f"Task {task.name} finished; {resource_usage}")
