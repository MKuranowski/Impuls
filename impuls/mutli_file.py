import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from logging import getLogger
from pathlib import Path
from typing import Any, Callable, Generic, Mapping, NamedTuple, Protocol, Type, TypedDict, TypeVar

from .errors import InputNotModified
from .model import Date
from .options import PipelineOptions
from .pipeline import Pipeline
from .resource import (
    LocalResource,
    ManagedResource,
    Resource,
    _download_resource,
    prepare_resources,
)
from .task import Task
from .tasks import TruncateCalendars, merge
from .tools.temporal import date_range
from .tools.types import Self

AnyResource = TypeVar("AnyResource", bound=Resource)

logger = getLogger("MultiFile")

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


class CachedFeedMetadata(TypedDict):
    """JSON object used to preserve IntermediateFeed data across runs,
    inside workspace/intermediate_inputs/metadata.json."""

    version: str
    start_date: str
    last_modified: float
    fetch_time: float


@dataclass(frozen=True)
class IntermediateFeed(Generic[AnyResource]):
    resource: AnyResource
    resource_name: str
    version: str
    start_date: Date

    def as_local_resource(self, stored_at: Path) -> "IntermediateFeed[LocalResource]":
        r = LocalResource(stored_at)
        r.last_modified = self.resource.last_modified
        r.fetch_time = self.resource.fetch_time
        return IntermediateFeed(r, self.resource_name, self.version, self.start_date)

    def as_cached_feed_metadata(self) -> CachedFeedMetadata:
        return {
            "version": self.version,
            "start_date": str(self.start_date),
            "last_modified": self.resource.last_modified.timestamp(),
            "fetch_time": self.resource.fetch_time.timestamp(),
        }

    @staticmethod
    def from_cached_feed_metadata(
        r: LocalResource,
        d: CachedFeedMetadata,
    ) -> "IntermediateFeed[LocalResource]":
        r.last_modified = datetime.fromtimestamp(d["last_modified"], timezone.utc)
        r.fetch_time = datetime.fromtimestamp(d["fetch_time"], timezone.utc)
        return IntermediateFeed(
            resource=r,
            resource_name=r.path.name,
            version=d["version"],
            start_date=Date.from_ymd_str(d["start_date"]),
        )


class IntermediateFeedProvider(Protocol[AnyResource]):
    def needed(self) -> list[IntermediateFeed[AnyResource]]:
        ...


AnyIntermediateFeed = IntermediateFeed[AnyResource]
AnyIntermediateFeedProvider = IntermediateFeedProvider[AnyResource]
TaskFactory = Callable[[IntermediateFeed[LocalResource]], list[Task]]
MultiTaskFactory = Callable[[list[IntermediateFeed[LocalResource]]], list[Task]]


def empty_tasks_factory(*_: Any) -> list[Task]:
    return []


@dataclass
class MultiFile(Generic[AnyResource]):
    intermediate_provider: IntermediateFeedProvider[AnyResource]
    intermediate_pipeline_tasks_factory: TaskFactory
    pre_merge_pipeline_tasks_factory: TaskFactory = empty_tasks_factory
    final_pipeline_tasks_factory: MultiTaskFactory = empty_tasks_factory

    additional_resources: Mapping[str, Resource] = field(default_factory=dict)
    options: PipelineOptions = PipelineOptions()

    merge_separator: str = ":"
    feed_version_separator: str = "/"
    distance_between_similar_stops_m: float = 10.0

    resources_have_changed: bool = False
    intermediates_have_changed: bool = False

    def prepare(self) -> Pipelines:
        # Dictionary:
        # - "intermediate": feed corresponding to a single input file
        # - "final": feed formed by merging intermediate databases
        # Used directories:
        # - workspace_directory/intermediate_inputs: directory with cached intermediate inputs
        # - workspace_directory/intermediate_dbs: directory with intermediate databases
        #
        # What needs to happen in the multi-file pipeline?

        intermediate_inputs_path = self.intermediate_inputs_path()
        resources = self.prepare_resources()

        # 1. Figure out which feeds are needed
        versions = self.resolve_versions()
        versions.log_result()

        # 2. Remove stale and no-longer-needed local feed inputs and databases
        versions.remove(intermediate_inputs_path)

        # 3. Fetch missing feed inputs
        local_fetched = versions.fetch(intermediate_inputs_path)
        local = versions.up_to_date + local_fetched

        # 3. Prepare intermediate pipelines for missing local feeds
        intermediates = self.prepare_intermediate_pipelines(local, resources)

        # 4. If there were no changes at all and not force_run - raise InputNotModified
        if not intermediates and not self.options.force_run:
            raise InputNotModified

        # 5. Prepare final pipeline for merging intermediate feeds
        final = self.prepare_final_pipeline(local, resources)

        return Pipelines(intermediates, final)

    def resolve_versions(self) -> "ResolvedVersions[AnyResource]":
        # If from_cache - resolve only based on locally cached files
        if self.options.from_cache:
            # NOTE: No changes to cached inputs will be made - no need to invalidate the cache
            cached = load_cached(self.options.workspace_directory)
            return ResolvedVersions(up_to_date=cached)

        needed = self.intermediate_provider.needed()
        cached = load_cached(self.options.workspace_directory)
        return ResolvedVersions.from_(needed, cached)

    def prepare_intermediate_pipelines(
        self,
        local: list[IntermediateFeed[LocalResource]],
        resources: Mapping[str, ManagedResource],
    ) -> list[Pipeline]:
        path = self.intermediate_dbs_path()
        version_and_expected_mod_time = {i.version: i.resource.last_modified for i in local}

        # Stale intermediate dbs need to be removed
        versions_up_to_date = set[str]()
        for db_file in path.iterdir():
            db_mod_time = datetime.fromtimestamp(db_file.stat().st_mtime, timezone.utc)
            expected_mod_time = version_and_expected_mod_time.get(db_file.stem, datetime.max)
            if db_mod_time < expected_mod_time:
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
        merge_task = merge.Merge(
            self.prepare_databases_to_merge(local, resources),
            separator=self.merge_separator,
            feed_version_separator=self.feed_version_separator,
            distance_between_similar_stops_m=self.distance_between_similar_stops_m,
        )

        pipeline = Pipeline(
            tasks=self.final_pipeline_tasks_factory(local),
            options=self.options,
        )
        pipeline.tasks.insert(0, merge_task)

        intermediate_dbs_path = self.intermediate_dbs_path()
        pipeline.managed_resources = {**resources}
        for feed in local:
            resource_name = f"{feed.version}.db"
            resource_path = intermediate_dbs_path / resource_name
            pipeline.managed_resources[resource_name] = ManagedResource(
                resource_path,
                datetime.fromtimestamp(resource_path.stat().st_mtime, timezone.utc),
                feed.resource.fetch_time,
            )

        return pipeline

    def prepare_databases_to_merge(
        self,
        local: list[IntermediateFeed[LocalResource]],
        resources: Mapping[str, ManagedResource],
    ) -> list[merge.DatabaseToMerge]:
        to_merge = list[merge.DatabaseToMerge]()

        for next_feed_idx, feed in enumerate(local, start=1):
            feed_start = feed.start_date
            feed_end = (
                None
                if next_feed_idx == len(local)
                else local[next_feed_idx].start_date.add_days(-1)
            )
            pre_merge_tasks = self.pre_merge_pipeline_tasks_factory(feed)
            pre_merge_tasks.insert(0, TruncateCalendars(date_range(feed_start, feed_end)))
            pre_merge_pipeline = Pipeline(pre_merge_tasks, options=self.options)
            pre_merge_pipeline.managed_resources = {**resources}

            to_merge.append(
                merge.DatabaseToMerge(f"{feed.version}.db", feed.version, pre_merge_pipeline)
            )

        return to_merge

    def prepare_resources(self) -> dict[str, ManagedResource]:
        raise NotImplementedError()  # TODO

    def intermediate_dbs_path(self) -> Path:
        p = self.options.workspace_directory / "intermediate_dbs"
        p.mkdir(exist_ok=True)
        return p

    def intermediate_inputs_path(self) -> Path:
        p = self.options.workspace_directory / "intermediate_inputs"
        p.mkdir(exist_ok=True)
        return p


@dataclass
class ResolvedVersions(Generic[AnyResource]):
    to_remove: list[IntermediateFeed[LocalResource]] = field(default_factory=list)
    """Subset of cached feeds which are no longer needed (version no longer needed) or
    stale (corresponding needed feed has a later last_modified).
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
        needed_modification_times = {i.version: i.resource.last_modified for i in needed}

        to_remove = [
            i
            for i in cached
            if i.version not in needed_modification_times
            or needed_modification_times[i.version] > i.resource.last_modified
        ]
        to_remove_versions = {i.version for i in to_remove}

        up_to_date = [i for i in cached if i.version not in to_remove_versions]
        up_to_date_versions = {i.version for i in up_to_date}

        to_fetch = [i for i in needed if i.version not in up_to_date_versions]

        return ResolvedVersions(to_remove, up_to_date, to_fetch)

    def log_result(self) -> None:
        to_remove_str = ", ".join(sorted(i.resource_name for i in self.to_remove))
        if len(self.to_remove) == 0:
            logger.info("0 cached input feeds are stale")
        elif len(self.to_remove) == 1:
            logger.info("1 cached input feed is stale:\n\t%s", to_remove_str)
        else:
            logger.info(
                "%d cached input feeds are stale:\n\t%s",
                len(self.to_remove),
                to_remove_str,
            )

        up_to_date_str = ", ".join(sorted(i.resource_name for i in self.up_to_date))
        if len(self.up_to_date) == 0:
            logger.info("0 cached input feeds is up-to-date")
        elif len(self.up_to_date) == 1:
            logger.info("1 cached input feed is up-to-date:\n\t%s", up_to_date_str)
        else:
            logger.info(
                "%d cached input feeds are up-to-date:\n\t%s",
                len(self.up_to_date),
                up_to_date_str,
            )

        to_fetch_str = ", ".join(sorted(i.resource_name for i in self.to_fetch))
        if len(self.to_fetch) == 0:
            logger.info("0 inputs feed need to be downloaded")
        elif len(self.to_fetch) == 1:
            logger.info("1 input feed needs to be downloaded:\n\t%s", to_fetch_str)
        else:
            logger.info(
                "%d cached input feeds need to be downloaded:\n\t%s",
                len(self.to_fetch),
                to_fetch_str,
            )

    def remove(self, intermediate_inputs_path: Path) -> None:
        for feed in self.to_remove:
            logger.debug("Removing stale feed input %s", feed.resource_name)
            remove_from_cache(intermediate_inputs_path, feed)

    def fetch(self, intermediate_inputs_path: Path) -> list[IntermediateFeed[LocalResource]]:
        local_fetched_feeds = list[IntermediateFeed[LocalResource]]()
        for feed in self.to_fetch:
            logger.debug("Downloading feed input ")
            local_feed = save_to_cache(intermediate_inputs_path, feed)
            local_fetched_feeds.append(local_feed)
        return local_fetched_feeds


def load_cached(intermediate_inputs_path: Path) -> list[IntermediateFeed[LocalResource]]:
    # Scan the intermediate_path directory for metadata files
    all_files = set(intermediate_inputs_path.iterdir())
    metadata_files = [f for f in all_files if f.is_file() and f.suffix == ".metadata"]
    recognized_files = set[Path]()

    # Go over metadata files and find corresponding content files
    feeds = list[IntermediateFeed[LocalResource]]()
    for metadata_file in metadata_files:
        content_file = metadata_file.with_suffix("")
        if content_file not in all_files:
            logger.error(
                "Intermediate inputs cache has %s, but no %s - assuming this feed is not cached",
                metadata_file,
                content_file,
            )
            continue

        recognized_files.add(metadata_file)
        recognized_files.add(content_file)

        with metadata_file.open("r") as f:
            metadata: CachedFeedMetadata = json.load(f)

        feed = IntermediateFeed.from_cached_feed_metadata(LocalResource(content_file), metadata)
        feeds.append(feed)

    # Remove unrecognized files
    unrecognized_files = all_files - recognized_files
    if unrecognized_files:
        logger.error(
            "Intermediate inputs cache has %d file(s) without corresponding .metadata - "
            "will attempt to remove it/them.",
            len(unrecognized_files),
        )
    for f in unrecognized_files:
        try:
            f.unlink()
            logger.error("Removed unrecognized file %s", f)
        except OSError as e:
            logger.error("Failed to remove %s:", f, exc_info=e)

    return feeds


def save_to_cache(
    intermediate_inputs_path: Path,
    feed: IntermediateFeed[AnyResource],
) -> IntermediateFeed[LocalResource]:
    # TODO: Check if the resource name can be used as a filename

    # Fetch the resource
    target_path = intermediate_inputs_path / feed.resource_name
    _download_resource(feed.resource, target_path)

    # Save its metadata
    metadata_path = intermediate_inputs_path / f"{feed.resource_name}.metadata"
    metadata = feed.as_cached_feed_metadata()
    with metadata_path.open("w") as f:
        json.dump(metadata, f)

    # Convert the feed to one using a LocalResource instead
    return feed.as_local_resource(target_path)


def remove_from_cache(
    intermediate_inputs_path: Path,
    cached: IntermediateFeed[LocalResource],
) -> None:
    if cached.resource.path.parent != intermediate_inputs_path / f"{cached.resource_name}":
        raise AssertionError(
            "save_cached expects feeds saved to the intermediate inputs cache directory"
        )

    metadata_path = intermediate_inputs_path / f"{cached.resource_name}.metadata"
    cached.resource.path.unlink()
    metadata_path.unlink()
