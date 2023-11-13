import logging
from pathlib import Path
from types import MappingProxyType
from typing import Mapping, Optional

from .db import DBConnection
from .errors import InputNotModified
from .options import PipelineOptions
from .resource import ManagedResource, Resource, prepare_resources
from .task import Task, TaskRuntime
from .tools import machine_load


class Pipeline:
    def __init__(
        self,
        tasks: list[Task],
        resources: Mapping[str, Resource] | None = None,
        options: PipelineOptions = PipelineOptions(),
        name: str = "",
        run_on_existing_db: bool = False,
    ) -> None:
        # Set parameters
        self.name: str = name
        self.logger: logging.Logger = logging.getLogger(f"{name}.Pipeline" if name else "Pipeline")
        self.raw_resources: Mapping[str, Resource] = resources or {}
        self.managed_resources: Optional[Mapping[str, ManagedResource]] = None
        self.tasks: list[Task] = tasks
        self.options: PipelineOptions = options
        self.run_on_existing_db: bool = run_on_existing_db

        # Update task loggers
        if self.name:
            for task in self.tasks:
                task.logger = logging.getLogger(f"{name}.Task.{task.name}")

        # Ensure the workspace directory exists
        self.options.workspace_directory.mkdir(parents=True, exist_ok=True)

        # Figure out the database path
        self.db_path: Path | None = None
        if self.options.save_db_in_workspace:
            self.db_path = self.options.workspace_directory / "impuls.db"
            # Remove the existing DB
            if not self.run_on_existing_db:
                self.db_path.unlink(missing_ok=True)

    def prepare_resources(self) -> None:
        if self.managed_resources is not None:
            # Resources are already prepared - no need to do anything
            return

        managed, should_continue = prepare_resources(
            self.raw_resources,
            self.options.workspace_directory,
            self.options.from_cache,
        )

        if not should_continue and not self.options.force_run:
            raise InputNotModified
        self.managed_resources = MappingProxyType(managed)

    def open_db(self) -> DBConnection:
        if not self.db_path:
            return DBConnection.create_with_schema(":memory:")
        elif self.run_on_existing_db and self.db_path.exists():
            return DBConnection(self.db_path)
        else:
            return DBConnection.create_with_schema(self.db_path)

    def run(self) -> None:
        # Ensure resources are ready to use
        self.prepare_resources()
        assert self.managed_resources is not None

        # Prepare the database
        with self.open_db() as db:
            # Prepare the runtime for tasks
            runtime = TaskRuntime(db, self.managed_resources, self.options)

            # Run the tasks
            for task in self.tasks:
                self.logger.info(f"Executing task {task.name}")
                with machine_load.LoadTracker() as resource_usage, db.transaction():
                    task.execute(runtime)
                self.logger.debug(f"Task {task.name} finished; {resource_usage}")
