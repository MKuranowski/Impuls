# © Copyright 2022-2024 Mikołaj Kuranowski
# SPDX-License-Identifier: GPL-3.0-or-later

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Mapping, Optional

from .db import DBConnection
from .options import PipelineOptions
from .resource import ManagedResource


@dataclass(frozen=True)
class TaskRuntime:
    """TaskRuntime is the argument passed to :py:meth:`Task.execute`,
    with the runtime environment for the task to act upon.
    """

    db: DBConnection
    resources: Mapping[str, ManagedResource]
    options: PipelineOptions


class Task(ABC):
    """Task is the fundamental block of a :py:class:`~impuls.Pipeline`,
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
        for Task implementations to hold some execute-related state, but that state should be
        reset on entry to execute.
        """
        raise NotImplementedError
