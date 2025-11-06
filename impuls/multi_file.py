# © Copyright 2022-2025 Mikołaj Kuranowski
# SPDX-License-Identifier: GPL-3.0-or-later

import json
from bisect import bisect_right
from dataclasses import dataclass, field
from datetime import datetime, timezone
from logging import getLogger
from operator import attrgetter
from pathlib import Path
from typing import Any, Callable, Generic, Mapping, NamedTuple, Protocol, TypedDict, TypeVar

from .errors import InputNotModified
from .model import Date
from .options import PipelineOptions
from .pipeline import Pipeline
from .resource import (
    DATETIME_MAX_UTC,
    DATETIME_MIN_UTC,
    LocalResource,
    ManagedResource,
    Resource,
    _download_resource,
    prepare_resources,
)
from .task import Task
from .tasks import TruncateCalendars, merge
from .tools.temporal import date_range

ResourceT = TypeVar("ResourceT", bound=Resource)
ResourceT_co = TypeVar("ResourceT_co", bound=Resource, covariant=True)

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
    """Pipelines creating all intermediate databases."""

    final: Pipeline
    """Final Pipeline, taking all intermediate databases and merging them all together."""

    def run(self) -> None:
        """run runs all pipelines in order"""
        for intermediate in self.intermediates:
            intermediate.run()
        self.final.run()


class CachedFeedMetadata(TypedDict):
    """JSON object used to preserve IntermediateFeed data across runs,
    stored in workspace/intermediate_inputs/{version}.metadata"""

    version: str
    start_date: str
    last_modified: float
    fetch_time: float


@dataclass(frozen=True)
class IntermediateFeed(Generic[ResourceT_co]):
    """IntermediateFeed represents self-contained schedules for a set period of time -
    a single version of timetables."""

    resource: ResourceT_co
    """resources represents arbitrary data containing schedule data by a
    :py:class:`~impuls.Resource`. This resource's last_modified time must be filled in by the
    :py:class:`~impuls.multi_file.IntermediateFeedProvider` - and must be available before the
    first call to fetch."""

    resource_name: str
    """resource_name is a string used for identifying the resource.
    This should be the version string plus an appropriate file extension."""

    version: str
    """version is an arbitrary string identifying the feed."""

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

    def as_cached_feed_metadata(self) -> CachedFeedMetadata:
        """Returns attributes of this IntermediateFeed as CachedFeedMetadata
        for preserving them across runs."""
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
        """Creates an IntermediateFeed from loaded metadata and a LocalResource."""
        r.last_modified = datetime.fromtimestamp(d["last_modified"], timezone.utc)
        r.fetch_time = datetime.fromtimestamp(d["fetch_time"], timezone.utc)
        return IntermediateFeed(
            resource=r,
            resource_name=r.path.name,
            version=d["version"],
            start_date=Date.from_ymd_str(d["start_date"]),
        )


class IntermediateFeedProvider(Protocol[ResourceT]):
    """IntermediateFeedProvider is an abstraction over an external repository of versioned
    schedules. The provider is responsible for communicating with the external repository
    and figuring out which feeds are needed to create a complete database.

    In most cases, this boils down to calling an external API enumerating
    all files required to form a coherent and continuous dataset, and returning a list
    of :py:class:`~impuls.multi_file.IntermediateFeed` with the same :py:class:`~impuls.Resource`
    type.
    """

    def needed(self) -> list[IntermediateFeed[ResourceT]]: ...


def prune_outdated_feeds(feeds: list[IntermediateFeed[ResourceT]], today: Date) -> None:
    """Removes feeds which end before ``today``."""
    feeds.sort(key=attrgetter("start_date"))

    # Find the feed corresponding to `self.for_date; see find_le in
    # https://docs.python.org/3/library/bisect.html#searching-sorted-lists
    cutoff_idx = max(
        bisect_right(
            feeds,
            today,
            key=attrgetter("start_date"),
        )
        - 1,
        0,
    )

    # Only return the needed feeds - those active on and after `self.for_date`
    del feeds[:cutoff_idx]


TaskFactory = Callable[[IntermediateFeed[LocalResource]], list[Task]]
MultiTaskFactory = Callable[[list[IntermediateFeed[LocalResource]]], list[Task]]


def empty_tasks_factory(*_: Any) -> list[Task]:
    """Returns an empty task list."""
    return []


@dataclass
class MultiFile(Generic[ResourceT_co]):
    """MultiFile prepares :py:class:`~impuls.multi_file.Pipelines` and multiple
    :py:class:`~impuls.Resource` objects for creating a single, continuous database,
    when the source data is available in multiple disjoint files.

    This is a solution to a common problem. Say the source data has the following files:

    * 2023-04-01.txt
    * 2023-04-17.txt
    * 2023-05-01.txt

    But the result is supposed to be a single GTFS feed.

    To be on the same page, further terminology needs to be introduced:

    * "intermediate" and "version" refer to sole, disjoined feed
    * "intermediate input" refers to an intermediate feed in an arbitrary format
    * "intermediate database" refers to an intermediate feed stored as an Impuls database
    * "final" refers to the coherent, merged feed

    MultiFile will preserve intermediate inputs across runs, avoiding re-downloading.
    Intermediate databases will also be preserved across runs - reducing the need
    to re-create them. If all :py:class:`~impuls.multi_file.IntermediateFeed` have not changed -
    :py:exc:`~impuls.errors.InputNotModified` will be raised and no actual work will be performed.

    Special folders in the workspace directory will be used to store intermediate inputs
    and intermediate databases. Running multiple programs accessing the same workspace
    can cause unexpected issues and is not supported.

    Unfortunately, as of now, MultiFile ignores changes in additional :py:class:`~impuls.Resource`
    - only changes in the intermediate inputs cause Pipelines to be created.

    Several Pipeline options change their meaning in the MultiFile context:

    * from_cache: nothing is ever fetched; additional_resources must be either cached or local,
      all cached intermediate inputs are used, bypassing the
      :py:class:`~impuls.multi_file.IntermediateFileProvider`. If the intermediate databases are
      up-to-date, only the final pipeline is created, unless force_run is also set to True.
    * force_run: any cached intermediate databases are ignored - in other words
      every intermediate input will have a corresponding intermediate pipeline created.
      The final pipeline is also created.

    The process of creating all of the necessary pipelines can be summarized in 5 steps:

    1. Figure out which intermediate feeds are needed
    2. Remove stale and no-longer-needed cached intermediate inputs and databases
    3. Fetch missing intermediate inputs
    4. Prepare intermediate pipelines for missing local feeds
    5. Prepare final pipeline for merging intermediate feeds
    """

    options: PipelineOptions
    """Options for the MultiFile process and created :py:class:`~impuls.multi_file.Pipelines`."""

    intermediate_provider: IntermediateFeedProvider[ResourceT_co]
    """intermediate_provider is responsible for calculating which intermediate feeds
    are required to create the final database."""

    intermediate_pipeline_tasks_factory: TaskFactory
    """Factory for tasks needed to turn an intermediate input to an intermediate database.

    The returned objects might be mutated - this function should always return
    new instances of Tasks in a new list.
    """

    pre_merge_pipeline_tasks_factory: TaskFactory = empty_tasks_factory
    """Factory for tasks applied right before merging. Runs as a sub-pipeline
    of the final pipeline, see :py:attr:`impuls.tasks.merge.DatabaseToMerge.pre_merge_pipeline`.

    A :py:class:`impuls.tasks.TruncateCalendars` task is prepended to the returned list,
    based on the start_date attribute of the next intermediate feed.

    The returned objects might be mutated - this function should always return
    new instances of Tasks in a new list.
    """

    final_pipeline_tasks_factory: MultiTaskFactory = empty_tasks_factory
    """Factory for tasks applied on the final pipeline.

    A :py:class:`~impuls.tasks.merge.Merge` task is prepended to the returned list,
    based on the needed intermediate feeds (as returned by a
    :py:class:`~impuls.multi_file.IntermediateFeedProvider`).

    The factory must not modified the provided list of intermediate feeds.

    The returned objects might be mutated - this function should always return
    new instances of Tasks in a new list.
    """

    additional_resources: Mapping[str, Resource] = field(default_factory=dict[str, Resource])
    """Additional resources, made available for all intermediate and final pipelines."""

    merge_separator: str = ":"
    """Passed through to the :py:class:`~impuls.tasks.merge.Merge` task -
    used for delimiting id fields and a unique prefix."""

    feed_version_separator: str = "/"
    """Passed through to the :py:class:`~impuls.tasks.merge.Merge` task -
    used for delimiting feed_version in a single FeedInfo object."""

    distance_between_similar_stops_m: float = 10.0
    """Passed through to the :py:class:`~impuls.tasks.merge.Merge` task -
    maximum distance for stops to be considered similar."""

    def prepare(self) -> Pipelines:
        """Returns the :py:class:`~impuls.multi_file.Pipelines` necessary to produce a single,
        merged feed.

        Raises :py:exc:`~impuls.errors.InputNotModified` if the intermediate inputs have not
        changed, barring other options like from_cache or force_run.
        """

        # 1. Load up additional resources
        self.options.workspace_directory.mkdir(parents=True, exist_ok=True)
        if self.additional_resources:
            resources = self._prepare_resources()
        else:
            resources = dict[str, ManagedResource]()

        cached = {i.version: i for i in _load_cached(self.intermediate_inputs_path())}
        local: list[IntermediateFeed[LocalResource]]
        updated: set[str]

        if not self.options.from_cache:
            # If not `from_cache`:
            # 2a. Get needed files from intermediate_feed_provider
            logger.info("Listing needed input files")
            needed = {i.version: i for i in self.intermediate_provider.needed()}

            # 3a. Remove unneeded files from intermediate_inputs_path
            self._remove_unneeded_cached_inputs(needed, cached)

            # 4a. Load last_modified and fetch_time from intermediate_inputs_path/*.metadata
            self._set_metadata_on_needed_files(needed, cached)

            # 5a. Run conditional fetches on all intermediate inputs
            local, updated = self._download_needed_inputs(needed, cached)

        else:
            # Otherwise (`from_cache`):
            # 2b. Create substitute IntermediateFeed[LocalResource] based on cached inputs
            logger.info("Loading cached input files")
            local = sorted(cached.values(), key=attrgetter("start_date"))
            updated = set()

        # 6. Prepare intermediate pipelines
        intermediates = self._prepare_intermediate_pipelines(local, resources, updated)
        if not intermediates and not self.options.from_cache:
            raise InputNotModified

        # 7. Create the final pipeline
        final = self._prepare_final_pipeline(local, resources)

        return Pipelines(intermediates, final)

    def _remove_unneeded_cached_inputs(
        self,
        needed: dict[str, IntermediateFeed[ResourceT_co]],
        cached: dict[str, IntermediateFeed[LocalResource]],
    ) -> None:
        to_remove: list[str] = [i for i in cached if i not in needed]
        for version in to_remove:
            feed = cached.pop(version)
            logger.info("Removing %s (file no longer needed)", feed.resource_name)
            _remove_from_cache(self.intermediate_inputs_path(), feed)

    def _set_metadata_on_needed_files(
        self,
        needed: dict[str, IntermediateFeed[ResourceT_co]],
        cached: dict[str, IntermediateFeed[LocalResource]],
    ) -> None:
        for needed_feed in needed.values():
            if cached_feed := cached.get(needed_feed.version):
                if needed_feed.resource_name != cached_feed.resource_name:
                    raise ValueError(
                        f"The resource name for feed version {needed_feed.version!r} "
                        f"has changed from {needed_feed.resource_name!r} to "
                        f"{cached_feed.resource_name!r}. This breaks input cache and is therefore "
                        " not allowed. Remove the intermediate_inputs directory manually to force "
                        " a fresh run."
                    )

                needed_feed.resource.last_modified = cached_feed.resource.last_modified
                needed_feed.resource.fetch_time = cached_feed.resource.fetch_time

    def _download_needed_inputs(
        self,
        needed: dict[str, IntermediateFeed[ResourceT_co]],
        cached: dict[str, IntermediateFeed[LocalResource]],
    ) -> tuple[list[IntermediateFeed[LocalResource]], set[str]]:
        local: list[IntermediateFeed[LocalResource]] = []
        changed: set[str] = set()

        for version, remote_feed in needed.items():
            if version in cached:
                conditional = True
                logger.info(
                    "Refreshing %s (downloading if it has changed)",
                    remote_feed.resource_name,
                )
            else:
                conditional = False
                logger.info("Downloading %s", remote_feed.resource_name)

            local_feed, has_changed = _save_to_cache(
                self.intermediate_inputs_path(),
                remote_feed,
                conditional,
            )
            local.append(local_feed)
            if has_changed:
                changed.add(version)

        local.sort(key=attrgetter("start_date"))
        return local, changed

    def _prepare_intermediate_pipelines(
        self,
        local: list[IntermediateFeed[LocalResource]],
        resources: Mapping[str, ManagedResource],
        force: set[str],
    ) -> list[Pipeline]:
        path = self.intermediate_dbs_path()
        version_and_expected_mod_time = {i.version: i.resource.last_modified for i in local}

        # Stale intermediate dbs need to be removed
        logger.info("Removing stale/unnecessary intermediate databases")
        versions_up_to_date = set[str]()
        for db_file in path.iterdir():
            # Ignore non-db files
            if db_file.suffix != ".db":
                logger.error("Unrecognized file in intermediate databases path: %s", db_file.name)
                continue

            db_mod_time = datetime.fromtimestamp(db_file.stat().st_mtime, timezone.utc)
            expected_mod_time = version_and_expected_mod_time.get(db_file.stem, DATETIME_MAX_UTC)
            if db_file.stem in force or db_mod_time < expected_mod_time:
                db_file.unlink()
            else:
                versions_up_to_date.add(db_file.stem)

        # Log how many intermediate databases are up to date,
        # unless force_run (in this case those dbs are ignored anyway)
        if not self.options.force_run:
            logger.info(
                "%d cached intermediate database%s up to date:\n\t%s",
                len(versions_up_to_date),
                " is" if len(versions_up_to_date) == 1 else "s are",
                ", ".join(sorted(versions_up_to_date)),
            )

        # Figure out which feeds need to be processed - there's no need to create pipeline
        # if we have an up-to-date db, unless force_run is enabled
        feeds_to_create = [
            feed
            for feed in local
            if self.options.force_run or feed.version not in versions_up_to_date
        ]
        logger.info(
            "%d intermediate pipeline%s need%s to be created:\n\t%s",
            len(feeds_to_create),
            "" if len(feeds_to_create) == 1 else "s",
            "s" if len(feeds_to_create) > 1 else "",
            ", ".join(sorted(feed.version for feed in feeds_to_create)),
        )

        # Prepare intermediate pipelines
        pipelines = list[Pipeline]()
        for feed in feeds_to_create:
            pipeline = Pipeline(
                tasks=self.intermediate_pipeline_tasks_factory(feed),
                options=self.options,
                name=feed.version,
                db_path=path / f"{feed.version}.db",
                remove_db_on_failure=True,
            )

            # Make the intermediate input and additional resources available
            pipeline.managed_resources = {**resources}
            pipeline.managed_resources[feed.resource_name] = ManagedResource(
                feed.resource.path,
                feed.resource.last_modified,
                feed.resource.fetch_time,
            )

            pipelines.append(pipeline)

        return pipelines

    def _prepare_final_pipeline(
        self,
        local: list[IntermediateFeed[LocalResource]],
        resources: Mapping[str, ManagedResource],
    ) -> Pipeline:
        logger.info("Preparing the final pipeline")
        merge_task = merge.Merge(
            self._prepare_databases_to_merge(local, resources),
            separator=self.merge_separator,
            feed_version_separator=self.feed_version_separator,
            distance_between_similar_stops_m=self.distance_between_similar_stops_m,
        )

        pipeline = Pipeline(
            tasks=self.final_pipeline_tasks_factory(local),
            options=self.options,
            name="Final",
        )
        pipeline.tasks.insert(0, merge_task)

        intermediate_dbs_path = self.intermediate_dbs_path()
        pipeline.managed_resources = {**resources}
        for feed in local:
            resource_name = f"{feed.version}.db"
            resource_path = intermediate_dbs_path / resource_name
            try:
                last_modified = datetime.fromtimestamp(resource_path.stat().st_mtime, timezone.utc)
            except FileNotFoundError:
                last_modified = DATETIME_MIN_UTC

            pipeline.managed_resources[resource_name] = ManagedResource(
                resource_path,
                last_modified,
                feed.resource.fetch_time,
            )

        return pipeline

    def _prepare_databases_to_merge(
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
            pre_merge_pipeline = Pipeline(
                pre_merge_tasks,
                options=self.options,
                name=f"{feed.version}.PreMerge",
            )
            pre_merge_pipeline.managed_resources = {**resources}

            to_merge.append(
                merge.DatabaseToMerge(f"{feed.version}.db", feed.version, pre_merge_pipeline)
            )

        return to_merge

    def _prepare_resources(self) -> dict[str, ManagedResource]:
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
        p.mkdir(exist_ok=True, parents=True)
        return p

    def intermediate_inputs_path(self) -> Path:
        p = self.options.workspace_directory / "intermediate_inputs"
        p.mkdir(exist_ok=True, parents=True)
        return p


def _load_cached(intermediate_inputs_path: Path) -> list[IntermediateFeed[LocalResource]]:
    """Loads all known cached intermediate inputs.
    Any unrecognized files will be removed and logged."""
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


def _save_to_cache(
    intermediate_inputs_path: Path,
    feed: IntermediateFeed[ResourceT],
    conditional: bool = True,
) -> tuple[IntermediateFeed[LocalResource], bool]:
    """Downloads the intermediate input into its cache."""
    # TODO: Check if the resource name can be used as a filename

    # Fetch the resource
    target_path = intermediate_inputs_path / feed.resource_name
    changed = True
    try:
        _download_resource(feed.resource, target_path, conditional)
    except InputNotModified:
        changed = False

    # Save its metadata
    metadata_path = intermediate_inputs_path / f"{feed.resource_name}.metadata"
    metadata = feed.as_cached_feed_metadata()
    with metadata_path.open("w") as f:
        json.dump(metadata, f)

    # Convert the feed to one using a LocalResource instead
    return feed.as_local_resource(target_path), changed


def _remove_from_cache(
    intermediate_inputs_path: Path,
    cached: IntermediateFeed[LocalResource],
) -> None:
    """Removes a cached intermediate input"""
    if cached.resource.path != intermediate_inputs_path / f"{cached.resource_name}":
        raise AssertionError(
            "save_cached expects feeds saved to the intermediate inputs cache directory"
        )

    metadata_path = intermediate_inputs_path / f"{cached.resource_name}.metadata"
    cached.resource.path.unlink()
    metadata_path.unlink()
