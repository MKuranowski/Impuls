# © Copyright 2022-2026 Mikołaj Kuranowski
# SPDX-License-Identifier: GPL-3.0-or-later

from . import db, errors, model, multi_file, resource, selector, tasks, tools
from .app import App
from .pipeline import Pipeline, PipelineOptions, Task, TaskRuntime
from .tools.logs import initialize as initialize_logging

__all__ = [
    "App",
    "DBConnection",
    "HTTPResource",
    "LocalResource",
    "Pipeline",
    "PipelineOptions",
    "Resource",
    "Task",
    "TaskRuntime",
    "db",
    "errors",
    "initialize_logging",
    "model",
    "multi_file",
    "resource",
    "selector",
    "tasks",
    "tools",
]

__title__ = "Impuls"
__description__ = "Framework for processing static public transportation data"
__url__ = "https://github.com/MKuranowski/Impuls"
__author__ = "Mikołaj Kuranowski"
__copyright__ = "© Copyright 2022-2026 Mikołaj Kuranowski"
__license__ = "GPL-3.0-or-later"
__version__ = "2.5.1"
__email__ = "mkuranowski+pypackages@gmail.com"

DBConnection = db.DBConnection
Resource = resource.Resource
HTTPResource = resource.HTTPResource
LocalResource = resource.LocalResource
