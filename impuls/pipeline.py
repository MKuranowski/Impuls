import logging
from argparse import ArgumentParser
from pathlib import Path
from typing import Any, NamedTuple, Protocol

from .db import DBConnection
from .resource import Resource, ResourceManager
from .tools import machine_load
from .tools.types import Self


class PipelineOptions(NamedTuple):
    # By default the resource manager will raise InputNotModified if all input resources
    # remained unchanged since last run. Setting this flag to True will force the pipeline
    # to always run.
    ignore_not_modified: bool = False

    # Directory where input resources are cached, and where tasks may store their workload
    # to preserve it across runs.
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

    def execute(
        self, db: DBConnection, options: PipelineOptions, resources: ResourceManager
    ) -> None:
        ...


class Pipeline:
    def __init__(
        self,
        tasks: list[Task],
        resources: list[Resource] | None = None,
        options: PipelineOptions = PipelineOptions(),
        name: str = "",
    ) -> None:
        # Set parameters
        self.name: str = name
        self.logger: logging.Logger = logging.getLogger(f"{name}.Pipeline" if name else "Pipeline")
        self.resources: ResourceManager = ResourceManager(resources or [])
        self.tasks: list[Task] = tasks
        self.options: PipelineOptions = options

        # Update task loggers
        if self.name:
            for task in self.tasks:
                task.logger = self.logger.getChild(task.logger.name)

        # Ensure the workspace directory exists
        self.options.workspace_directory.mkdir(parents=True, exist_ok=True)

        # Open the database
        if self.options.save_db_in_workspace:
            db_path_obj = self.options.workspace_directory / "impuls.db"
            db_path_obj.unlink(missing_ok=True)
            db_path = str(db_path_obj)
        else:
            db_path = ":memory:"

        self.db: DBConnection = DBConnection.create_with_schema(db_path)

    def close(self) -> None:
        self.db.close()

    def __enter__(self: Self) -> Self:
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    def run(self) -> None:
        self.resources.cache_resources(
            self.options.workspace_directory,
            self.options.ignore_not_modified,
        )

        for task in self.tasks:
            self.logger.info(f"Executing task {task.name}")

            with machine_load.LoadTracker() as resource_usage, self.db.transaction():
                task.execute(self.db, self.options, self.resources)

            self.logger.debug(f"Task {task.name} finished; {resource_usage}")
