from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from logging import getLogger
from pathlib import Path
from typing import Callable, Generic, Mapping, NamedTuple, Protocol, Type, TypeVar

from .errors import InputNotModified
from .options import PipelineOptions
from .pipeline import Pipeline
from .resource import LocalResource, ManagedResource, Resource
from .task import Task
from .tasks import SaveDB, merge
from .tools.types import Self

AnyResource = TypeVar("AnyResource", bound=Resource)

logger = getLogger(__name__)

# Dictionary:
# - "intermediate": feed corresponding to a single input file
# - "final": feed formed by merging intermediate databases
#
# What needs to happen in the multi-file pipeline?
# 1. Figure out which feeds are needed
#    If from_cache - based on locally cached files
# 2. Remove stale and no-longer-needed local feed inputs and databases
#    If from_cache - nothing will change
# 3. Fetch missing feed inputs and other resources
#    If from_cache - nothing new will be pulled in
# 3. Prepare intermediate pipelines for missing local feeds
#    If force_run - for all feeds
#    Else if from_cache - for no feeds
# 4. If there were no changes at all and not force_run - raise InputNotModified
# 5. Prepare final pipeline for merging intermediate feeds
#
# Used directories:
# - workspace_directory/intermediate_inputs: directory with cached intermediate input resources
# - workspace_directory/intermediate_dbs: directory with intermediate databases


class Pipelines(NamedTuple):
    intermediates: list[Pipeline]
    final: Pipeline

    def run(self) -> None:
        for intermediate in self.intermediates:
            intermediate.run()
        self.final.run()


@dataclass(frozen=True)
class IntermediateFeed(Generic[AnyResource]):
    resource: AnyResource
    resource_name: str
    version: str
    start_date: date
    update_time: datetime

    def fetch(self) -> "IntermediateFeed[LocalResource]":
        raise NotImplementedError  # TODO


class IntermediateFeedProvider(Protocol[AnyResource]):
    def needed(self) -> list[IntermediateFeed[AnyResource]]:
        ...


AnyIntermediateFeed = IntermediateFeed[AnyResource]
AnyIntermediateFeedProvider = IntermediateFeedProvider[AnyResource]


@dataclass
class MultiFile(Generic[AnyResource]):
    intermediate_provider: IntermediateFeedProvider[AnyResource]
    intermediate_pipeline_tasks_factory: Callable[[IntermediateFeed[LocalResource]], list[Task]]
    final_pipeline_tasks_factory: Callable[[list[IntermediateFeed[LocalResource]]], list[Task]]
    additional_resources: Mapping[str, Resource] = field(default_factory=dict)
    options: PipelineOptions = PipelineOptions()

    merge_separator: str = ":"
    feed_version_separator: str = "/"
    distance_between_similar_stops_m: float = 10.0

    def prepare(self) -> Pipelines:
        # Dictionary:
        # - "intermediate": feed corresponding to a single input file
        # - "final": feed formed by merging intermediate databases
        # Used directories:
        # - workspace_directory/intermediate_inputs: directory with cached intermediate inputs
        # - workspace_directory/intermediate_dbs: directory with intermediate databases
        #
        # What needs to happen in the multi-file pipeline?

        resources = self.prepare_resources()

        # 1. Figure out which feeds are needed
        #    If from_cache - based on locally cached files
        versions: ResolvedVersions[AnyResource]
        if not self.options.from_cache:
            needed = self.intermediate_provider.needed()
            cached = load_cached(self.options.workspace_directory)
            versions = ResolvedVersions.from_(needed, cached)
        else:
            needed = []
            cached = load_cached(self.options.workspace_directory)
            versions = ResolvedVersions(up_to_date=cached)

        # 2. Remove stale and no-longer-needed local feed inputs and databases
        versions.remove()

        # 3. Fetch missing feed inputs and other resources
        local = versions.fetch()

        # 3. Prepare intermediate pipelines for missing local feeds
        intermediate_pipelines = self.prepare_intermediate_pipelines(local, resources)

        # 4. If there were no changes at all and not force_run - raise InputNotModified
        if not intermediate_pipelines and not self.options.force_run:
            raise InputNotModified

        # 5. Prepare final pipeline for merging intermediate feeds
        raise NotImplementedError

    def prepare_intermediate_pipelines(
        self,
        local: list[IntermediateFeed[LocalResource]],
        resources: Mapping[str, ManagedResource],
    ) -> list[Pipeline]:
        path = self.intermediate_dbs_path()
        version_and_expected_update_time = {i.version: i.update_time for i in local}

        # Stale intermediate dbs need to be removed
        versions_up_to_date = set[str]()
        for db_file in path.iterdir():
            db_update_time = datetime.fromtimestamp(db_file.stat().st_mtime, timezone.utc)
            expected_update_time = version_and_expected_update_time.get(db_file.stem, datetime.max)
            if db_update_time < expected_update_time:
                db_file.unlink()
            else:
                versions_up_to_date.add(db_file.stem)

        # Prepare pipelines
        pipelines = list[Pipeline]()
        for feed in local:
            # No need to create pipeline if we have an up-to-date db, unless force_run is enabled
            if feed.version in versions_up_to_date and not self.options.force_run:
                continue

            pipeline = Pipeline(
                tasks=self.intermediate_pipeline_tasks_factory(feed),
                options=self.options,
            )
            pipeline.db_path = path / f"{feed.version}.db"
            pipeline.managed_resources = {**resources}
            pipeline.managed_resources[feed.resource_name] = ManagedResource(
                feed.resource.path,
                feed.resource.last_modified,
                feed.resource.fetch_time,
            )
            pipelines.append(pipeline)

        return pipelines

    def prepare_final_pipeline(
        self,
        local: list[IntermediateFeed[LocalResource]],
        resources: Mapping[str, ManagedResource],
    ) -> Pipeline:
        raise NotImplementedError("TODO: Trim intermediate schedules")

        merge_task = merge.Merge(
            [merge.DatabaseToMerge(f"{i.version}.db", i.version) for i in local],
            separator=self.merge_separator,
            feed_version_separator=self.feed_version_separator,
            distance_between_similar_stops_m=self.distance_between_similar_stops_m,
        )

        pipeline = Pipeline(
            tasks=self.final_pipeline_tasks_factory(local),
            options=self.options,
        )
        pipeline.tasks.insert(0, merge_task)

        intermedate_dbs_path = self.intermediate_dbs_path()
        pipeline.managed_resources = {**resources}
        for feed in local:
            resource_name = f"{feed.version}.db"
            resource_path = intermedate_dbs_path / resource_name
            pipeline.managed_resources[resource_name] = ManagedResource(
                resource_path,
                datetime.fromtimestamp(resource_path.stat().st_mtime, timezone.utc),
                feed.resource.fetch_time,
            )

        return pipeline

    def prepare_resources(self) -> dict[str, ManagedResource]:
        raise NotImplementedError()  # TODO

    def intermediate_dbs_path(self) -> Path:
        p = self.options.workspace_directory / "intermediate_dbs"
        p.mkdir(exist_ok=True)
        return p


@dataclass
class ResolvedVersions(Generic[AnyResource]):
    to_remove: list[IntermediateFeed[LocalResource]] = field(default_factory=list)
    """Subset of cached feeds which are no longer needed (version no longer needed) or
    stale (corresponding needed feed has a later update_time).
    """

    up_to_date: list[IntermediateFeed[LocalResource]] = field(default_factory=list)
    """Subset of cached feeds which are needed and up-to-date."""

    to_fetch: list[IntermediateFeed[AnyResource]] = field(default_factory=list)
    """Subset of needed feeds which need to be pulled and processed."""

    @classmethod
    def from_(
        cls: Type[Self],
        needed: list[IntermediateFeed[AnyResource]],
        cached: list[IntermediateFeed[LocalResource]],
    ) -> Self:
        needed_update_times = {i.version: i.update_time for i in needed}

        to_remove = [
            i
            for i in cached
            if i.version not in needed_update_times
            or needed_update_times[i.version] > i.update_time
        ]
        to_remove_versions = {i.version for i in to_remove}

        up_to_date = [i for i in cached if i.version not in to_remove_versions]
        up_to_date_versions = {i.version for i in up_to_date}

        to_fetch = [i for i in needed if i.version not in up_to_date_versions]

        return ResolvedVersions(to_remove, up_to_date, to_fetch)

    def remove(self) -> None:
        raise NotImplementedError()  # TODO

    def fetch(self) -> list[IntermediateFeed[LocalResource]]:
        raise NotImplementedError()  # TODO


def load_cached(workspace: Path) -> list[IntermediateFeed[LocalResource]]:
    raise NotImplementedError()  # TODO


def save_cached(workspace: Path, cached: list[IntermediateFeed[LocalResource]]) -> None:
    raise NotImplementedError()  # TODO
