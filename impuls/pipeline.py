# © Copyright 2022-2025 Mikołaj Kuranowski
# SPDX-License-Identifier: GPL-3.0-or-later

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
from .tools.types import StrPath


class Pipeline:
    """Pipeline encapsulates the process of downloading and processing multiple
    resources by a sequence of tasks.

    :param list[Task] tasks: List of :py:class:`~impuls.Task` instances to be executed in the
        Pipeline
    :param Mapping[str, Resource] | None resources: Additional :py:class:`~impuls.Resource`
        instances to be made available to the tasks being executed, by their name.
        Defaults to no additional resources.
    :param PipelineOptions options: Detailed options controlling the behavior of the Pipeline,
        usually controllable by the end-user. See the documentation for the class itself for
        more details.
    :param str name: Prefix to be used by Pipeline and Task loggers. Defaults to no prefix.
    :param StrPath | None db_path: Path where the SQLite database with data should be stored.
        Defaults to impuls.db inside of the workspace directory. For advanced usage only, the
        :py:class:`~impuls.tasks.SaveDB` task should be used.
    :param bool run_on_existing_db: Don't clear the database before executing the Tasks;
        effectively assuming that the database stored at ``db_path`` exists and has
        the expected schema. Advanced usage only.
    :param bool remove_db_on_failure: Remove the database file when the Pipeline fails.
    """

    def __init__(
        self,
        tasks: list[Task],
        resources: Mapping[str, Resource] | None = None,
        options: PipelineOptions = PipelineOptions(),
        name: str = "",
        db_path: StrPath | None = None,
        run_on_existing_db: bool = False,
        remove_db_on_failure: bool = False,
    ) -> None:
        # Set parameters
        self.name: str = name
        self.logger: logging.Logger = logging.getLogger(f"{name}.Pipeline" if name else "Pipeline")
        self.raw_resources: Mapping[str, Resource] = resources or {}
        self.managed_resources: Optional[Mapping[str, ManagedResource]] = None
        self.tasks: list[Task] = tasks
        self.options: PipelineOptions = options
        self.run_on_existing_db: bool = run_on_existing_db
        self.remove_db_on_failure: bool = remove_db_on_failure

        # Update task loggers
        if self.name:
            for task in self.tasks:
                task.logger = logging.getLogger(f"{name}.Task.{task.name}")

        # Ensure the workspace directory exists
        self.options.workspace_directory.mkdir(parents=True, exist_ok=True)

        # Figure out the database path
        self.db_path: Path = (
            Path(db_path) if db_path else self.options.workspace_directory / "impuls.db"
        )

    def prepare_resources(self) -> None:
        """prepare_resources ensures that all resources are cached and available locally.
        Raises :py:exc:`~impuls.errors.InputNotModified` if none of the resources have changed
        since previous run, or :py:exc:`~impuls.errors.MultipleDataErrors` with
        :py:exc:`~impuls.errors.ResourceNotCached`.
        """
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
        """open_db opens a :py:class:`~impuls.DBConnection` to an empty database
        stored in the workspace, following the Impuls :py:mod:`~impuls.model`.

        Except that the database may not be stored in the workspace nor it may be empty,
        but this is reserved for advanced usage only.
        """
        if self.run_on_existing_db and self.db_path.exists():
            return DBConnection(self.db_path)
        else:
            if not self.run_on_existing_db:
                self.db_path.unlink(missing_ok=True)
            return DBConnection.create_with_schema(self.db_path)

    def run(self) -> None:
        """run ensures all resources are cached and then executes all tasks
        on a fresh database.
        """

        # Ensure resources are ready to use
        self.prepare_resources()
        assert self.managed_resources is not None

        # Prepare the database
        try:
            with self.open_db() as db:
                # Prepare the runtime for tasks
                runtime = TaskRuntime(db, self.managed_resources, self.options)

                # Run the tasks
                for task in self.tasks:
                    self.logger.info(f"Executing task {task.name}")
                    with machine_load.LoadTracker() as resource_usage:
                        task.execute(runtime)
                    self.logger.debug(f"Task {task.name} finished; {resource_usage}")
                self.logger.info("All tasks finished")
        except Exception:
            if self.remove_db_on_failure:
                self.db_path.unlink(missing_ok=True)
            raise
