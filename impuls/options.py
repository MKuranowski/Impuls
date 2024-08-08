# © Copyright 2022-2024 Mikołaj Kuranowski
# SPDX-License-Identifier: GPL-3.0-or-later

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class PipelineOptions:
    """PipelineOptions control the behavior of :py:class:`~impuls.Pipeline`."""

    force_run: bool = False
    """force_run, when set to ``True``, suppresses the :py:exc:`~impuls.errors.InputNotModified`
    error and forces the :py:class:`~impuls.Pipeline` to run.

    The default value is ``False``, and :py:class:`~impuls.Pipeline` raises
    :py:exc:`~impuls.errors.InputNotModified` if all resources were not modified.

    This option has no effect if there are no resources or :py:attr:`from_cache` is set -
    in those cases the :py:class:`~impuls.Pipeline` runs unconditionally.
    """

    from_cache: bool = False
    """from_cache, when set to ``True``, causes the Pipeline to never fetch any resource,
    forcing to use locally cached ones. If any :py:class:`~impuls.Resource` is not cached,
    :py:exc:`~impuls.errors.MultipleDataError` with :py:exc:`~impuls.errors.ResourceNotCached`
    will be raised.

    Default value is ``False``.

    Forces the pipeline to run.
    """

    workspace_directory: Path = Path("_impuls_workspace")
    """workspace_directory controls the directory where input resources are cached,
    and where tasks may store their workload to preserve it across runs.

    If the given directory doesn't exist, :py:class:`~impuls.Pipeline` attempts to create it
    (and its parents).
    """
