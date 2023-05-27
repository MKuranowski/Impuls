import logging
from typing import Any, Mapping, Optional

from .db import DBConnection
from .errors import InputNotModified
from .options import PipelineOptions
from .resource import ManagedResource, Resource, cache_resources, ensure_resources_cached
from .task import Task, TaskRuntime
from .tools import machine_load
from .tools.types import Self


class Pipeline:
    def __init__(
        self,
        tasks: list[Task],
        resources: Mapping[str, Resource] | None = None,
        options: PipelineOptions = PipelineOptions(),
        name: str = "",
    ) -> None:
        # Set parameters
        self.name: str = name
        self.logger: logging.Logger = logging.getLogger(f"{name}.Pipeline" if name else "Pipeline")
        self.raw_resources: Mapping[str, Resource] = resources or {}
        self.managed_resources: Optional[Mapping[str, ManagedResource]] = None
        self.tasks: list[Task] = tasks
        self.options: PipelineOptions = options

        # Update task loggers
        if self.name:
            for task in self.tasks:
                task.logger.name = f"{name}.Task.{task.name}"

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

    def prepare_resources(self) -> None:
        if self.managed_resources is not None:
            # Resources are already prepared - no need to do anything
            return
        elif self.options.from_cache:
            # Asked not to download any resources - just ensure they're all cached
            self.managed_resources = ensure_resources_cached(
                self.raw_resources,
                self.options.workspace_directory,
            )
        elif self.options.force_run:
            # Force pipeline run - ignore InputNotModified
            try:
                self.managed_resources = cache_resources(
                    self.raw_resources,
                    self.options.workspace_directory,
                )
            except InputNotModified:
                self.managed_resources = ensure_resources_cached(
                    self.raw_resources,
                    self.options.workspace_directory,
                )
        else:
            # Normal case - download outdated resources or propagate InputNotModified
            self.managed_resources = cache_resources(
                self.raw_resources,
                self.options.workspace_directory,
            )

    def run(self) -> None:
        # Ensure resources are ready to use
        self.prepare_resources()
        assert self.managed_resources is not None

        # Prepare the runtime for tasks
        runtime = TaskRuntime(self.db, self.managed_resources, self.options)

        # Run the tasks
        for task in self.tasks:
            self.logger.info(f"Executing task {task.name}")
            with machine_load.LoadTracker() as resource_usage, self.db.transaction():
                task.execute(runtime)
            self.logger.debug(f"Task {task.name} finished; {resource_usage}")
