import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Optional

from .db import DBConnection
from .errors import InputNotModified
from .resource import ManagedResource, Resource, cache_resources, ensure_resources_cached
from .tools import machine_load
from .tools.types import Self


@dataclass(frozen=True)
class PipelineOptions:
    force_run: bool = False
    """By default pipeline raises InputNotModified if all resources were not modified.
    Setting this flag to True suppresses the error and forces the pipeline to run.

    This option has no option if there are no resources or from_cache is set - in those cases
    the pipeline runs unconditionally.
    """

    from_cache: bool = False
    """Causes the Pipeline to never fetch any resource, forcing to use locally cached ones.
    If any Resource is not cached, MultipleDataError with ResourceNotCached will be raised.

    Has no effect if there are no resources, and forces the pipeline to run.
    """

    workspace_directory: Path = Path("_impuls_workspace")
    """Directory where input resources are cached, and where tasks may store their workload
    to preserve it across runs.

    If the given directory doesn't exist, pipeline attempts to create it (and its parents).
    """

    save_db_in_workspace: bool = False
    """By default Impuls saves the sqlite DB in-memory.
    Setting this flag to true causes the DB to be saved in the workspace directory instead.
    """


@dataclass(frozen=True)
class TaskRuntime:
    """TaskRuntime is the argument passed to Task.execute,
    with the runtime environment for the task to act upon.
    """

    db: DBConnection
    resources: Mapping[str, ManagedResource]
    options: PipelineOptions


class Task(ABC):
    """Task is the fundamental block of a Pipeline,
    responsible for actually working on the data.
    """

    name: str
    logger: logging.Logger

    def __init__(self, name: Optional[str] = None) -> None:
        self.name = name or type(self).__name__
        self.logger = logging.getLogger(f"Task.{self.name}")

    @abstractmethod
    def execute(self, r: TaskRuntime) -> None:
        """execute process the data in the runtime environment.

        As of now, Tasks are guaranteed to run in a single thread with a single runtime,
        but execute may be called multiple times in different runtime. Thus, it is safe
        for Task sub-classes to hold some execute-related state, but that state should be
        reset on entry to execute.
        """
        raise NotImplementedError


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
                pass
        else:
            # Normal case - download outdated resources or propagate InputNotModified
            self.managed_resources = cache_resources(
                self.raw_resources,
                self.options.workspace_directory,
            )

    def run(self) -> None:
        # Ensure resources are ready to use
        if self.managed_resources is None:
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
