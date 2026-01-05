# © Copyright 2022-2024 Mikołaj Kuranowski
# SPDX-License-Identifier: GPL-3.0-or-later

from . import db, errors, model, multi_file, resource, selector, tasks, tools
from .app import App
from .pipeline import Pipeline, PipelineOptions, Task, TaskRuntime
from .tools.logs import initialize as initialize_logging

__all__ = [
    "db",
    "errors",
    "model",
    "multi_file",
    "resource",
    "selector",
    "tasks",
    "tools",
    "App",
    "DBConnection",
    "Pipeline",
    "PipelineOptions",
    "Task",
    "TaskRuntime",
    "HTTPResource",
    "LocalResource",
    "Resource",
    "initialize_logging",
]

__title__ = "Impuls"
__description__ = "Framework for processing static public transportation data"
__url__ = "https://github.com/MKuranowski/Impuls"
__author__ = "Mikołaj Kuranowski"
__copyright__ = "© Copyright 2022-2025 Mikołaj Kuranowski"
__license__ = "GPL-3.0-or-later"
__version__ = "2.4.1"
__email__ = "mkuranowski+pypackages@gmail.com"

DBConnection = db.DBConnection
Resource = resource.Resource
HTTPResource = resource.HTTPResource
LocalResource = resource.LocalResource
