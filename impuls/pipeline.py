# © Copyright 2022-2026 Mikołaj Kuranowski
# SPDX-License-Identifier: GPL-3.0-or-later

import logging
from collections.abc import Iterable, Mapping
from pathlib import Path
from types import MappingProxyType

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
        options: PipelineOptions = PipelineOptions(),  # noqa: B008
        name: str = "",
        db_path: StrPath | None = None,
        run_on_existing_db: bool = False,
        remove_db_on_failure: bool = False,
    ) -> None:
        # Set parameters
        self.name: str = name
        self.logger: logging.Logger = logging.getLogger(f"{name}.Pipeline" if name else "Pipeline")
        self.raw_resources: Mapping[str, Resource] = resources or {}
        self.managed_resources: Mapping[str, ManagedResource] | None = None
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

        # Force this pipeline to run if previous run had fresh files and failed;
        # and force further pipelines to run if we have fresh files, until this run succeeds.
        is_implied = ImpliedRun(self.options.workspace_directory).is_set()
        files_changed = should_continue and self.raw_resources and not self.options.from_cache
        if files_changed:
            ImpliedRun(self.options.workspace_directory).set()

        if not should_continue and not self.options.force_run and not is_implied:
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
                ImpliedRun(self.options.workspace_directory).clear()
        except Exception:
            if self.remove_db_on_failure:
                self.db_path.unlink(missing_ok=True)
            raise


class ImpliedRun:
    """ImpliedRun manages a persistent flag, which implies if a :py:class:`Pipeline` should run.

    For example, when a Pipeline pulls new resources, but then subsequently fails;
    the next run will be *implied* to run. This is to avoid spurious
    :py:class:`~impuls.errors.InputNotModified` if the output was not fully processed.

    Persistence is achieved by storing a ``implied_run.txt`` file in the workspace directory.
    """

    def __init__(self, workspace: StrPath) -> None:
        self.file = Path(workspace, "implied_run.txt")

    def set(self, intermediate_versions: Iterable[str] = ()) -> None:
        """Sets the subsequent runs to be implied, creating the persistent file.

        Optional ``intermediate_versions`` can be set to force specific
        :py:class:`~impuls.multi_file.MultiFile` intermediate pipelines to run as well.
        """

        with self.file.open("w", encoding="utf-8") as f:
            for version in intermediate_versions:
                f.write(version)
                f.write("\n")

    def clear(self) -> None:
        """Completely removes the flag to imply any subsequent runs."""
        self.file.unlink(missing_ok=True)

    def is_set(self) -> bool:
        """Checks if the implied run flag is set."""
        return self.file.exists()

    def implied_versions(self) -> "set[str]":
        """Returns the names of all ``intermediate_versions`` that were provided to the last call
        to :py:meth:`set`.

        Note that it's possible that ``implied_run.is_set() and len(implied_run.implied_versions()) == 0``,
        which would only imply the final pipeline to run.
        """
        try:
            return set(self.file.read_text("utf-8").splitlines())
        except FileNotFoundError:
            return set()
