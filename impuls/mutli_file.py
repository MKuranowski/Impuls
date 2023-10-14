import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from logging import getLogger
from pathlib import Path
from typing import (
    Any,
    Callable,
    Generic,
    Mapping,
    NamedTuple,
    Protocol,
    Sequence,
    Type,
    TypedDict,
    TypeVar,
)

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


class Pipelines(NamedTuple):
    """Pipelines is the result of resolving multi-file feed.

    The final pipeline creates a single merged database from feeds
    indicated by an IntermediateFeedProvider.

    Intermediate pipelines are used to create databases corresponding
    to missing individual feeds. The intermediate pipelines export
    database resources required by the final pipeline.
    """

    intermediates: list[Pipeline]
    final: Pipeline

    def run(self) -> None:
        """run runs all pipelines in order"""
        for intermediate in self.intermediates:
            intermediate.run()
        self.final.run()


class _CachedFeedMetadata(TypedDict):
    """JSON object used to preserve IntermediateFeed data across runs,
    stored in workspace/intermediate_inputs/{version}.metadata"""

    version: str
    start_date: str
    last_modified: float
    fetch_time: float


@dataclass(frozen=True)
class IntermediateFeed(Generic[AnyResource]):
    """IntermediateFeed represents self-contained schedules for a set period of time -
    a single version of schedules."""

    resource: AnyResource
    """resource represents arbitrary data containing schedule data"""

    resource_name: str
    """resource_name is a string used for identifying the resource.
    This should be the version string plus an appropriate file extension."""

    version: str
    """version is an arbitrary† string identifying the feed.
    († however, this string will be used in filenames)
    """

    start_date: Date
    """start_date represents the first day for which this feed's schedules are valid"""

    def as_local_resource(self, stored_at: Path) -> "IntermediateFeed[LocalResource]":
        """as_local_resource returns the same IntermediateFeed, but with
        the resource replaced by a LocalResource stored at the provided path.
        Resource metadata (last_modified and fetch_time) are also copied.
        """
        r = LocalResource(stored_at)
        r.last_modified = self.resource.last_modified
        r.fetch_time = self.resource.fetch_time
        return IntermediateFeed(r, self.resource_name, self.version, self.start_date)

    def as_cached_feed_metadata(self) -> _CachedFeedMetadata:
        """Returns attributes of this IntermediateFeed as CachedFeedMetadata
        for preserving them across runs"""
        return {
            "version": self.version,
            "start_date": str(self.start_date),
            "last_modified": self.resource.last_modified.timestamp(),
            "fetch_time": self.resource.fetch_time.timestamp(),
        }

    @staticmethod
    def from_cached_feed_metadata(
        r: LocalResource,
        d: _CachedFeedMetadata,
    ) -> "IntermediateFeed[LocalResource]":
        """Creates an IntermediateFeed from loaded metadata and a LocalResource"""
        r.last_modified = datetime.fromtimestamp(d["last_modified"], timezone.utc)
        r.fetch_time = datetime.fromtimestamp(d["fetch_time"], timezone.utc)
        return IntermediateFeed(
            resource=r,
            resource_name=r.path.name,
            version=d["version"],
            start_date=Date.from_ymd_str(d["start_date"]),
        )


class IntermediateFeedProvider(Protocol[AnyResource]):
    """IntermediateFeedProvider is an abstraction over an external repository of versioned
    schedules. The provider is responsible for communicating with the external repository
    and figuring out which feeds are needed to create a complete database."""

    def needed(self) -> list[IntermediateFeed[AnyResource]]:
        ...


TaskFactory = Callable[[IntermediateFeed[LocalResource]], list[Task]]
MultiTaskFactory = Callable[[Sequence[IntermediateFeed[LocalResource]]], list[Task]]


def empty_tasks_factory(*_: Any) -> list[Task]:
    return []


@dataclass
class MultiFile(Generic[AnyResource]):
    """MultiFile prepares Pipelines and Resources for creating a single,
    continuous database, when the source data is available in multiple disjoint files.

    This is a solution to a common problem. Say the source data has the following files:
    - 2023-04-01.txt
    - 2023-04-17.txt
    - 2023-05-01.txt
    But the result is supposed to be a single GTFS feed.

    To be on the same page, further terminology needs to be introduced:
    - "intermediate" and "version" refer to sole, disjoined feed
    - "intermediate input" refers to an intermediate feed in an arbitrary
        format
    - "intermediate database" refers to an intermediate feed stored as an Impuls database
    - "final" refers to the coherent, merged feed

    MultiFile will preserve intermediate inputs across runs, avoiding re-downloading.
    Intermediate databases will also be preserved across runs - reducing the need
    to re-create them. If the intermediate feeds have not changed - InputNotModified will
    be raised and no actual work will be performed.

    Special folders in the workspace directory will be used to store intermediate inputs
    and intermediate databases. Running multiple programs accessing the same workspace
    can cause unexpected issues and is not supported.

    Unfortunately, as of now, MultiFile ignores changes in Resources - only changes in
    the intermediate inputs cause Pipelines to be created.

    Several Pipeline options change their meaning in the MultiFile context:
    - from_cache: nothing is ever fetched: additional_resources must be either cached or local,
        all cached intermediate inputs are used, bypassing the intermediate provider.
        Only the final pipeline is created, unless force_run is also set to True.
    - force_run: any cached intermediate databases are ignored - in other words
        every intermediate input will have a corresponding intermediate pipeline created.
        The final pipeline is also created.

    The process of creating all of the necessary pipelines can be summarized in 5 steps:
    1. Figure out which intermediate feeds are needed
    2. Remove stale and no-longer-needed cached intermediate inputs and databases
    3. Fetch missing intermediate inputs
    4. Prepare intermediate pipelines for missing local feeds
    5. Prepare final pipeline for merging intermediate feeds
    """

    intermediate_provider: IntermediateFeedProvider[AnyResource]
    """intermediate_provider is responsible for calculating which intermediate feeds
    are required to create the final database."""

    intermediate_pipeline_tasks_factory: TaskFactory
    """Factory for tasks needed to turn an intermediate input to an intermediate database.

    The returned objects might be mutated - this function should always return
    new instances of Tasks in a new list.
    """

    pre_merge_pipeline_tasks_factory: TaskFactory = empty_tasks_factory
    """Factory for tasks applied right before merging. Runs as a sub-pipeline
    of the final pipeline, see :py:attr:`merge.DatabaseToMerge.pre_merge_pipeline`.

    A :py:class:`TruncateCalendars` task is prepended to the returned list,
    based on the start_date attribute of the next intermediate feed.

    The returned objects might be mutated - this function should always return
    new instances of Tasks in a new list.
    """

    final_pipeline_tasks_factory: MultiTaskFactory = empty_tasks_factory
    """Factory for tasks applied on the final pipeline.

    A :py:class:`merge.Merge` task is prepended to the returned list,
    based on the needed intermediate feeds (as returned by a :py:class:`IntermediateFeedProvider`).

    The factory must not modified the provided list of intermediate feeds.

    The returned objects might be mutated - this function should always return
    new instances of Tasks in a new list.
    """

    additional_resources: Mapping[str, Resource] = field(default_factory=dict)
    """Additional resources, made available for all intermediate and final pipelines"""

    options: PipelineOptions = PipelineOptions()
    """Options for returned Pipelines."""

    merge_separator: str = ":"
    """Passed through to :py:class:`merge.Merge` -
    used for delimiting id fields and a unique prefix"""

    feed_version_separator: str = "/"
    """Passed through to :py:class:`merge.Merge` -
    used for delimiting feed_version in a single FeedInfo object"""

    distance_between_similar_stops_m: float = 10.0
    """Passed through to :py:class:`merge.Merge` -
    maximum distance for stops to be considered similar"""

    def prepare(self) -> Pipelines:
        """Returns the pipelines necessary to produce a single, merged feed.

        Raises InputNotModified if the intermediate inputs have not changed,
        barring other options like from_cache or force_run.
        """
        intermediate_inputs_path = self.intermediate_inputs_path()

        if self.additional_resources:
            resources = self.prepare_resources()
        else:
            resources = dict[str, ManagedResource]()

        # 1. Figure out which intermediate feeds are needed
        versions = self.resolve_versions()
        versions.log_result()

        # 2. Remove stale and no-longer-needed cached intermediate inputs and databases
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

    def resolve_versions(self) -> "_ResolvedVersions[AnyResource]":
        # If from_cache - resolve only based on locally cached files
        if self.options.from_cache:
            # NOTE: No changes to cached inputs will be made - no need to invalidate the cache
            logger.warning("Using cached intermediate feeds")
            cached = _load_cached(self.options.workspace_directory)
            return _ResolvedVersions(up_to_date=cached)

        logger.info("Checking needed intermediate feeds")
        needed = self.intermediate_provider.needed()
        cached = _load_cached(self.options.workspace_directory)
        return _ResolvedVersions.from_(needed, cached)

    def prepare_intermediate_pipelines(
        self,
        local: list[IntermediateFeed[LocalResource]],
        resources: Mapping[str, ManagedResource],
    ) -> list[Pipeline]:
        path = self.intermediate_dbs_path()
        version_and_expected_mod_time = {i.version: i.resource.last_modified for i in local}

        # Stale intermediate dbs need to be removed
        logger.info("Removing stale/unnecessary intermediate databases")
        versions_up_to_date = set[str]()
        for db_file in path.iterdir():
            db_mod_time = datetime.fromtimestamp(db_file.stat().st_mtime, timezone.utc)
            expected_mod_time = version_and_expected_mod_time.get(db_file.stem, datetime.max)
            if db_mod_time < expected_mod_time:
                db_file.unlink()
            else:
                versions_up_to_date.add(db_file.stem)

        # Log how many intermediate databases are up to date,
        # unless force_run (in this case those dbs are ignored anyway)
        if not self.options.force_run:
            logger.info(
                "%d cached intermediate database%s up to date",
                len(versions_up_to_date),
                " is" if len(versions_up_to_date) == 1 else "s are",
            )

        # Figure out which feeds need to be processed - there's no need to create pipeline
        # if we have an up-to-date db, unless force_run is enabled
        feeds_to_create = [
            feed
            for feed in local
            if self.options.force_run or feed.version not in versions_up_to_date
        ]
        logger.info(
            "%d intermediate pipeline%s need to be created",
            len(feeds_to_create),
            "" if len(feeds_to_create) == 1 else "s",
        )

        # Prepare intermediate pipelines
        pipelines = list[Pipeline]()
        for feed in feeds_to_create:
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
        logger.info("Preparing the final pipeline")
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
        logger.info("Preparing additional resources")
        # NOTE: Any changes in the additional resources are completely ignored:
        #       Pipeline might not run, even if the intermediate resources have changed.
        r, _ = prepare_resources(
            self.additional_resources,
            self.options.workspace_directory,
            self.options.from_cache,
        )
        return r

    def intermediate_dbs_path(self) -> Path:
        p = self.options.workspace_directory / "intermediate_dbs"
        p.mkdir(exist_ok=True)
        return p

    def intermediate_inputs_path(self) -> Path:
        p = self.options.workspace_directory / "intermediate_inputs"
        p.mkdir(exist_ok=True)
        return p


@dataclass
class _ResolvedVersions(Generic[AnyResource]):
    """ResolvedVersions groups both cached and external versions by the action that needs to be taken."""

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
        """Resolves versions based on all needed and all cached feeds."""
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

        return _ResolvedVersions(to_remove, up_to_date, to_fetch)

    def log_result(self) -> None:
        """Logs the result of version resolution"""
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
            logger.info("0 cached input feeds are up-to-date")
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
            logger.info("0 input feeds need to be downloaded")
        elif len(self.to_fetch) == 1:
            logger.info("1 input feed needs to be downloaded:\n\t%s", to_fetch_str)
        else:
            logger.info(
                "%d cached input feeds need to be downloaded:\n\t%s",
                len(self.to_fetch),
                to_fetch_str,
            )

    def remove(self, intermediate_inputs_path: Path) -> None:
        """Removes all to_remove feeds"""
        if self.to_remove:
            logger.info(
                "Removing %d stale/unnecessary intermediate input%s",
                len(self.to_remove),
                "s" if len(self.to_remove) > 1 else "",
            )

        for feed in self.to_remove:
            logger.debug("Removing %s", feed.resource_name)
            _remove_from_cache(intermediate_inputs_path, feed)

    def fetch(self, intermediate_inputs_path: Path) -> list[IntermediateFeed[LocalResource]]:
        """Fetches all to_fetch feeds"""
        if self.to_fetch:
            logger.info(
                "Downloading %d intermediate input%s",
                len(self.to_fetch),
                "s" if len(self.to_fetch) > 1 else "",
            )

        local_fetched_feeds = list[IntermediateFeed[LocalResource]]()
        for feed in self.to_fetch:
            logger.debug("Downloading %s", feed.resource_name)
            local_feed = _save_to_cache(intermediate_inputs_path, feed)
            local_fetched_feeds.append(local_feed)
        return local_fetched_feeds


def _load_cached(intermediate_inputs_path: Path) -> list[IntermediateFeed[LocalResource]]:
    """Loads all known cached intermediate inputs. Any unrecognized files will be removed and reported."""
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
            metadata: _CachedFeedMetadata = json.load(f)

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


def _save_to_cache(
    intermediate_inputs_path: Path,
    feed: IntermediateFeed[AnyResource],
) -> IntermediateFeed[LocalResource]:
    """Downloads the intermediate input into its cache"""
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


def _remove_from_cache(
    intermediate_inputs_path: Path,
    cached: IntermediateFeed[LocalResource],
) -> None:
    """Removes a cached intermediate input"""
    if cached.resource.path.parent != intermediate_inputs_path / f"{cached.resource_name}":
        raise AssertionError(
            "save_cached expects feeds saved to the intermediate inputs cache directory"
        )

    metadata_path = intermediate_inputs_path / f"{cached.resource_name}.metadata"
    cached.resource.path.unlink()
    metadata_path.unlink()
