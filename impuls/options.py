from dataclasses import dataclass
from pathlib import Path


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
