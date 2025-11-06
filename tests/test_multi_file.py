import json
import os
from datetime import datetime, timedelta, timezone
from operator import attrgetter
from pathlib import Path
from typing import Literal, cast
from unittest import TestCase
from unittest.mock import Mock

from impuls import LocalResource, Pipeline, PipelineOptions, Task, TaskRuntime
from impuls.errors import InputNotModified, MultipleDataErrors, ResourceNotCached
from impuls.model import Date
from impuls.multi_file import (
    CachedFeedMetadata,
    IntermediateFeed,
    IntermediateFeedProvider,
    MultiFile,
    Pipelines,
    _load_cached,
    _remove_from_cache,
    _save_to_cache,
    prune_outdated_feeds,
)
from impuls.tasks import TruncateCalendars, merge
from impuls.tools.temporal import date_range
from impuls.tools.testing_mocks import MockDatetimeNow, MockFile, MockResource


class TestPipelines(TestCase):
    @staticmethod
    def pipeline_with_mock_run() -> Pipeline:
        p = Pipeline([])
        p.run = Mock()
        return p

    def test_run(self) -> None:
        p = Pipelines(
            [self.pipeline_with_mock_run(), self.pipeline_with_mock_run()],
            self.pipeline_with_mock_run(),
        )
        p.run()

        cast(Mock, p.intermediates[0].run).assert_called_once()
        cast(Mock, p.intermediates[1].run).assert_called_once()
        cast(Mock, p.final.run).assert_called_once()


class TestIntermediateFeed(TestCase):
    def test_as_local_resource(self) -> None:
        f = IntermediateFeed(
            MockResource(
                b"",
                datetime.fromisoformat("2023-04-02T12:57:00+00:00"),
                datetime.fromisoformat("2023-04-01T11:10:00+00:00"),
            ),
            "foo_v1.txt",
            "v1",
            Date(2023, 4, 1),
        )
        p = Path("/tmp/foo_v1.txt")
        local_f = f.as_local_resource(p)

        self.assertEqual(local_f.resource.path, p)
        self.assertEqual(
            local_f.resource.fetch_time,
            datetime.fromisoformat("2023-04-02T12:57:00+00:00"),
        )
        self.assertEqual(
            local_f.resource.last_modified,
            datetime.fromisoformat("2023-04-01T11:10:00+00:00"),
        )
        self.assertEqual(local_f.resource_name, "foo_v1.txt")
        self.assertEqual(local_f.version, "v1")
        self.assertEqual(local_f.start_date, Date(2023, 4, 1))

    def test_as_cached_metadata(self) -> None:
        f = IntermediateFeed(
            MockResource(
                b"",
                datetime.fromisoformat("2023-04-02T12:57:00+00:00"),
                datetime.fromisoformat("2023-04-01T11:10:00+00:00"),
            ),
            "foo_v1.txt",
            "v1",
            Date(2023, 4, 1),
        )
        d = f.as_cached_feed_metadata()

        self.assertDictEqual(
            d,
            {
                "version": "v1",
                "start_date": "2023-04-01",
                "last_modified": 1680347400.0,
                "fetch_time": 1680440220.0,
            },
        )

    def test_from_cached_metadata(self) -> None:
        p = Path("/tmp/foo_v1.txt")
        f = IntermediateFeed.from_cached_feed_metadata(
            LocalResource(p),
            {
                "version": "v1",
                "start_date": "2023-04-01",
                "last_modified": 1680347400.0,
                "fetch_time": 1680440220.0,
            },
        )

        self.assertEqual(f.resource.path, p)
        self.assertEqual(
            f.resource.fetch_time,
            datetime.fromisoformat("2023-04-02T12:57:00+00:00"),
        )
        self.assertEqual(
            f.resource.last_modified,
            datetime.fromisoformat("2023-04-01T11:10:00+00:00"),
        )
        self.assertEqual(f.resource_name, "foo_v1.txt")
        self.assertEqual(f.version, "v1")
        self.assertEqual(f.start_date, Date(2023, 4, 1))

    def test_prune_outdated_feeds(self) -> None:
        feeds = [
            IntermediateFeed(MockResource(), "v4.txt", "v4", Date(2023, 5, 14)),
            IntermediateFeed(MockResource(), "v3.txt", "v3", Date(2023, 5, 1)),
            IntermediateFeed(MockResource(), "v2.txt", "v2", Date(2023, 4, 20)),
            IntermediateFeed(MockResource(), "v1.txt", "v1", Date(2023, 4, 1)),
        ]
        today = Date(2023, 4, 30)
        prune_outdated_feeds(feeds, today)

        self.assertEqual(len(feeds), 3)
        self.assertEqual(feeds[0].version, "v2")
        self.assertEqual(feeds[1].version, "v3")
        self.assertEqual(feeds[2].version, "v4")


class TestCache(TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.mock_intermediate_inputs = MockFile(directory=True)
        self.d = self.mock_intermediate_inputs.path

    def tearDown(self) -> None:
        super().tearDown()
        self.mock_intermediate_inputs.cleanup()

    def test_load_cached(self) -> None:
        (self.d / "v1.txt").write_bytes(b"Foo\n")
        (self.d / "v1.txt.metadata").write_text(
            json.dumps(
                {
                    "version": "v1",
                    "start_date": "2023-04-01",
                    "last_modified": datetime(2023, 3, 30, tzinfo=timezone.utc).timestamp(),
                    "fetch_time": datetime(2023, 4, 2, 11, 15, tzinfo=timezone.utc).timestamp(),
                }
            )
        )

        (self.d / "v2.txt").write_bytes(b"Bar\n")
        (self.d / "v2.txt.metadata").write_text(
            json.dumps(
                {
                    "version": "v2",
                    "start_date": "2023-04-14",
                    "last_modified": datetime(2023, 3, 31, tzinfo=timezone.utc).timestamp(),
                    "fetch_time": datetime(2023, 4, 2, 11, 16, tzinfo=timezone.utc).timestamp(),
                }
            )
        )

        f = _load_cached(self.d)
        f.sort(key=attrgetter("version"))  # ensure consistent ordering for tests
        self.assertEqual(len(f), 2)

        self.assertEqual(f[0].resource.path, self.d / "v1.txt")
        self.assertEqual(f[0].resource.last_modified, datetime(2023, 3, 30, tzinfo=timezone.utc))
        self.assertEqual(
            f[0].resource.fetch_time,
            datetime(2023, 4, 2, 11, 15, tzinfo=timezone.utc),
        )
        self.assertEqual(f[0].resource_name, "v1.txt")
        self.assertEqual(f[0].version, "v1")
        self.assertEqual(f[0].start_date, Date(2023, 4, 1))

        self.assertEqual(f[1].resource.path, self.d / "v2.txt")
        self.assertEqual(f[1].resource.last_modified, datetime(2023, 3, 31, tzinfo=timezone.utc))
        self.assertEqual(
            f[1].resource.fetch_time,
            datetime(2023, 4, 2, 11, 16, tzinfo=timezone.utc),
        )
        self.assertEqual(f[1].resource_name, "v2.txt")
        self.assertEqual(f[1].version, "v2")
        self.assertEqual(f[1].start_date, Date(2023, 4, 14))

    def test_load_cached_removes_without_metadata(self) -> None:
        (self.d / "v1.txt").write_bytes(b"Foo\n")
        f = _load_cached(self.d)
        self.assertEqual(len(f), 0)
        self.assertFalse((self.d / "v1.txt").exists())

    def test_load_cached_removes_metadata_only(self) -> None:
        (self.d / "v1.txt.metadata").write_text(
            json.dumps(
                {
                    "version": "v1",
                    "start_date": "2023-04-01",
                    "last_modified": datetime(2023, 3, 30, tzinfo=timezone.utc).timestamp(),
                    "fetch_time": datetime(2023, 4, 2, 11, 15, tzinfo=timezone.utc).timestamp(),
                }
            )
        )
        f = _load_cached(self.d)
        self.assertEqual(len(f), 0)
        self.assertFalse((self.d / "v1.txt.metadata").exists())

    def test_save(self) -> None:
        mock_fetch_time = datetime(2023, 4, 2, 11, 15, tzinfo=timezone.utc)
        in_feed = IntermediateFeed(
            resource=MockResource(
                b"Foo\n",
                last_modified=datetime(2023, 3, 30, tzinfo=timezone.utc),
                clock=MockDatetimeNow.constant(mock_fetch_time).now,
            ),
            resource_name="v1.txt",
            version="v1",
            start_date=Date(2023, 4, 1),
        )
        out_feed, _ = _save_to_cache(self.d, in_feed)

        self.assertEqual(out_feed.resource.path, self.d / "v1.txt")
        self.assertEqual(out_feed.resource.path.read_bytes(), b"Foo\n")
        self.assertEqual(out_feed.resource.fetch_time, mock_fetch_time)
        self.assertEqual(out_feed.resource.last_modified, in_feed.resource.last_modified)
        self.assertEqual(out_feed.resource_name, "v1.txt")
        self.assertEqual(out_feed.version, "v1")
        self.assertEqual(out_feed.start_date, Date(2023, 4, 1))

        with (self.d / "v1.txt.metadata").open(mode="r") as metadata_fp:
            metadata: CachedFeedMetadata = json.load(metadata_fp)

        self.assertDictEqual(
            metadata,
            {
                "version": "v1",
                "start_date": "2023-04-01",
                "last_modified": in_feed.resource.last_modified.timestamp(),
                "fetch_time": mock_fetch_time.timestamp(),
            },
        )

    def test_remove(self) -> None:
        (self.d / "v1.txt").write_bytes(b"Foo\n")
        (self.d / "v1.txt.metadata").write_text(
            json.dumps(
                {
                    "version": "v1",
                    "start_date": "2023-04-01",
                    "last_modified": datetime(2023, 3, 30, tzinfo=timezone.utc).timestamp(),
                    "fetch_time": datetime(2023, 4, 2, 11, 15, tzinfo=timezone.utc).timestamp(),
                }
            )
        )

        f = IntermediateFeed(
            LocalResource(self.d / "v1.txt"),
            "v1.txt",
            "v1",
            Date(2023, 4, 1),
        )
        _remove_from_cache(self.d, f)

        self.assertFalse((self.d / "v1.txt").exists())
        self.assertFalse((self.d / "v1.txt.metadata").exists())


def mock_feed(version: str) -> IntermediateFeed[MockResource]:
    match version:
        case "v1":
            return IntermediateFeed(
                resource=MockResource(),
                resource_name="v1.txt",
                version="v1",
                start_date=Date(2023, 4, 1),
            )
        case "v2":
            return IntermediateFeed(
                resource=MockResource(),
                resource_name="v2.txt",
                version="v2",
                start_date=Date(2023, 4, 14),
            )
        case "v3":
            return IntermediateFeed(
                resource=MockResource(),
                resource_name="v3.txt",
                version="v3",
                start_date=Date(2023, 5, 1),
            )
        case _:
            raise ValueError(f"invalid mock feed version: {version}")


class MockIntermediateFeedProvider(IntermediateFeedProvider[MockResource]):
    def __init__(self, r: list[IntermediateFeed[MockResource]] | None = None) -> None:
        self.r = r or [mock_feed("v2"), mock_feed("v3")]

    def needed(self) -> list[IntermediateFeed[MockResource]]:
        return self.r


class DummyTask(Task):
    def execute(self, r: TaskRuntime) -> None:
        pass


class BrokenTask(Task):
    def execute(self, r: TaskRuntime) -> None:
        raise ValueError


def mock_task_factory(feed: IntermediateFeed[LocalResource]) -> list[Task]:
    return [DummyTask(f"DummyTask.{feed.version}")]


def mock_broken_task_factory(feed: IntermediateFeed[LocalResource]) -> list[Task]:
    return [BrokenTask()]


def mock_multi_task_factory(feeds: list[IntermediateFeed[LocalResource]]) -> list[Task]:
    return [DummyTask("DummyTask.multiple")]


class TestMultiFile(TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.workspace = MockFile(directory=True)
        self.options = PipelineOptions(workspace_directory=self.workspace.path)
        self.multi_file = MultiFile(
            options=self.options,
            intermediate_provider=MockIntermediateFeedProvider(),
            intermediate_pipeline_tasks_factory=mock_task_factory,
            final_pipeline_tasks_factory=mock_multi_task_factory,
        )

    def tearDown(self) -> None:
        super().tearDown()
        self.workspace.cleanup()

    @staticmethod
    def mock_last_modified_time(version: str) -> datetime:
        match version:
            case "v1":
                return datetime(2023, 3, 28, tzinfo=timezone.utc)
            case "v2":
                return datetime(2023, 3, 30, tzinfo=timezone.utc)
            case "v3":
                return datetime(2023, 4, 5, tzinfo=timezone.utc)
            case _:
                raise ValueError(f"invalid mock feed version: {version!r}")

    def mock_input(self, version: str, last_modified: datetime | None = None) -> None:
        last_modified = last_modified or self.mock_last_modified_time(version)
        fetch_time = last_modified + timedelta(hours=8)

        p = self.multi_file.intermediate_inputs_path()
        (p / f"{version}.txt").touch()
        with (p / f"{version}.txt.metadata").open(mode="w") as f:
            metadata = mock_feed(version).as_cached_feed_metadata()
            metadata["last_modified"] = last_modified.timestamp()
            metadata["fetch_time"] = fetch_time.timestamp()
            json.dump(metadata, f)

    def mock_db(self, version: str, last_modified: datetime | None = None) -> None:
        p = self.multi_file.intermediate_dbs_path()
        f = p / f"{version}.db"
        f.touch()

        t = (last_modified or self.mock_last_modified_time(version)).timestamp()
        os.utime(f, (t, t))

    def check_intermediate_pipeline(self, p: Pipeline, version: str) -> None:
        self.assertEqual(p.options, self.options)

        db_path = self.workspace.path / "intermediate_dbs" / f"{version}.db"
        self.assertEqual(p.db_path, db_path)
        self.assertTrue(p.remove_db_on_failure)

        assert p.managed_resources is not None
        self.assertEqual(len(p.managed_resources), 1)
        self.assertIn(f"{version}.txt", p.managed_resources)

        self.assertEqual(len(p.tasks), 1)
        self.assertIsInstance(p.tasks[0], DummyTask)
        self.assertEqual(p.tasks[0].name, f"DummyTask.{version}")

    def check_pre_merge_tasks(
        self,
        p: Pipeline | None,
        version: Literal["v2", "v3"],
        has_pre_merge_dummy_tasks: bool = False,
    ) -> None:
        self.assertIsNotNone(p)
        assert p is not None  # for type checker

        self.assertEqual(p.options, self.options)

        self.assertEqual(len(p.tasks), 2 if has_pre_merge_dummy_tasks else 1)

        self.assertIsInstance(p.tasks[0], TruncateCalendars)
        truncate_task = cast(TruncateCalendars, p.tasks[0])
        if version == "v2":
            self.assertEqual(
                truncate_task.target,
                date_range(Date(2023, 4, 14), Date(2023, 4, 30)),
            )
        else:
            self.assertEqual(
                truncate_task.target,
                date_range(Date(2023, 5, 1), None),
            )

        if has_pre_merge_dummy_tasks:
            self.assertIsInstance(p.tasks[1], DummyTask)
            self.assertEqual(p.tasks[1].name, f"DummyTask.{version}")

    def check_merge_task(self, t: merge.Merge, has_pre_merge_dummy_tasks: bool = False) -> None:
        self.assertEqual(len(t.databases_to_merge), 2)

        self.assertEqual(t.databases_to_merge[0].resource_name, "v2.db")
        self.assertEqual(t.databases_to_merge[0].prefix, "v2")
        self.check_pre_merge_tasks(
            t.databases_to_merge[0].pre_merge_pipeline,
            "v2",
            has_pre_merge_dummy_tasks,
        )

        self.assertEqual(t.databases_to_merge[1].resource_name, "v3.db")
        self.assertEqual(t.databases_to_merge[1].prefix, "v3")
        self.check_pre_merge_tasks(
            t.databases_to_merge[1].pre_merge_pipeline,
            "v3",
            has_pre_merge_dummy_tasks,
        )

    def check_final_pipeline(self, p: Pipeline, has_pre_merge_dummy_tasks: bool = False) -> None:
        self.assertEqual(p.options, self.options)

        assert p.managed_resources is not None
        self.assertEqual(len(p.managed_resources), 2)
        self.assertIn("v2.db", p.managed_resources)
        self.assertIn("v3.db", p.managed_resources)

        self.assertEqual(len(p.tasks), 2)
        self.assertIsInstance(p.tasks[0], merge.Merge)
        self.check_merge_task(cast(merge.Merge, p.tasks[0]), has_pre_merge_dummy_tasks)

        self.assertIsInstance(p.tasks[1], DummyTask)
        self.assertEqual(p.tasks[1].name, "DummyTask.multiple")

    def test(self) -> None:
        intermediates, final = self.multi_file.prepare()

        self.assertEqual(len(intermediates), 2)
        self.check_intermediate_pipeline(intermediates[0], "v2")
        self.check_intermediate_pipeline(intermediates[1], "v3")
        self.check_final_pipeline(final, has_pre_merge_dummy_tasks=False)

    def test_skips_cached_dbs(self) -> None:
        self.mock_input("v2")
        self.mock_db("v2")

        intermediates, final = self.multi_file.prepare()

        self.assertEqual(len(intermediates), 1)
        self.check_intermediate_pipeline(intermediates[0], "v3")
        self.check_final_pipeline(final, has_pre_merge_dummy_tasks=False)

    def test_overwrites_cached_db_if_fetched(self) -> None:
        # NOTE: This test is particularly nasty, simulating a situation
        #       when the input resource has changed after a fetch, but before generating a db
        v2_resource = (
            cast(
                MockIntermediateFeedProvider,
                self.multi_file.intermediate_provider,
            )
            .r[0]
            .resource
        )
        v2_resource.persistent_last_modified = datetime(2023, 3, 30, tzinfo=timezone.utc)

        self.mock_input("v2", datetime(2023, 3, 29, tzinfo=timezone.utc))
        self.mock_db("v2", datetime(2023, 3, 31, tzinfo=timezone.utc))

        intermediates, final = self.multi_file.prepare()

        self.assertEqual(len(intermediates), 2)
        self.check_intermediate_pipeline(intermediates[0], "v2")
        self.check_intermediate_pipeline(intermediates[1], "v3")
        self.check_final_pipeline(final, has_pre_merge_dummy_tasks=False)

    def test_removes_stale_inputs(self) -> None:
        self.mock_input("v1")
        self.mock_db("v1")

        self.multi_file.prepare()

        self.assertFalse((self.multi_file.intermediate_inputs_path() / "v1.txt").exists())
        self.assertFalse((self.multi_file.intermediate_dbs_path() / "v1.db").exists())

    def test_raises_input_not_modified(self) -> None:
        self.mock_input("v2")
        self.mock_input("v3")
        self.mock_db("v2")
        self.mock_db("v3")

        with self.assertRaises(InputNotModified):
            self.multi_file.prepare()

    def test_pre_merge_pipeline(self) -> None:
        self.multi_file.pre_merge_pipeline_tasks_factory = mock_task_factory
        intermediates, final = self.multi_file.prepare()

        self.assertEqual(len(intermediates), 2)
        self.check_intermediate_pipeline(intermediates[0], "v2")
        self.check_intermediate_pipeline(intermediates[1], "v3")
        self.check_final_pipeline(final, has_pre_merge_dummy_tasks=True)

    def test_additional_resources(self) -> None:
        self.multi_file.additional_resources = {"foo.txt": MockResource(b"Foo\n")}
        intermediate, final = self.multi_file.prepare()

        # Check that intermediate pipelines have access to the additional resource

        assert intermediate[0].managed_resources is not None
        self.assertIn("foo.txt", intermediate[0].managed_resources)
        self.assertEqual(
            self.workspace.path / "foo.txt",
            intermediate[0].managed_resources["foo.txt"].stored_at,
        )
        self.assertEqual(b"Foo\n", intermediate[0].managed_resources["foo.txt"].bytes())

        assert intermediate[1].managed_resources is not None
        self.assertIn("foo.txt", intermediate[0].managed_resources)
        self.assertEqual(
            self.workspace.path / "foo.txt",
            intermediate[1].managed_resources["foo.txt"].stored_at,
        )

        # Check that the pre-merge pipelines have access to the additional resource

        assert isinstance(final.tasks[0], merge.Merge)
        to_merge = final.tasks[0].databases_to_merge
        assert to_merge[0].pre_merge_pipeline is not None
        assert to_merge[0].pre_merge_pipeline.managed_resources is not None
        assert to_merge[1].pre_merge_pipeline is not None
        assert to_merge[1].pre_merge_pipeline.managed_resources is not None

        self.assertIn("foo.txt", to_merge[0].pre_merge_pipeline.managed_resources)
        self.assertEqual(
            self.workspace.path / "foo.txt",
            to_merge[0].pre_merge_pipeline.managed_resources["foo.txt"].stored_at,
        )

        self.assertIn("foo.txt", to_merge[1].pre_merge_pipeline.managed_resources)
        self.assertEqual(
            self.workspace.path / "foo.txt",
            to_merge[1].pre_merge_pipeline.managed_resources["foo.txt"].stored_at,
        )

        # Check that the final pipeline has access to the additional resource

        assert final.managed_resources is not None
        self.assertIn("foo.txt", intermediate[0].managed_resources)
        self.assertEqual(
            self.workspace.path / "foo.txt",
            intermediate[0].managed_resources["foo.txt"].stored_at,
        )

    def test_force_run(self) -> None:
        self.options = PipelineOptions(workspace_directory=self.workspace.path, force_run=True)
        self.multi_file.options = self.options

        self.mock_input("v2")
        self.mock_db("v2")
        self.mock_input("v3")
        self.mock_db("v3")

        intermediates, final = self.multi_file.prepare()

        self.assertEqual(len(intermediates), 2)
        self.check_intermediate_pipeline(intermediates[0], "v2")
        self.check_intermediate_pipeline(intermediates[1], "v3")
        self.check_final_pipeline(final, has_pre_merge_dummy_tasks=False)

    def test_from_cache(self) -> None:
        self.options = PipelineOptions(workspace_directory=self.workspace.path, from_cache=True)
        self.multi_file.options = self.options
        self.multi_file.intermediate_provider.needed = Mock()

        self.mock_input("v2")
        self.mock_db("v2")
        self.mock_input("v3")
        self.mock_db("v3")

        intermediates, final = self.multi_file.prepare()

        self.assertEqual(len(intermediates), 0)
        self.check_final_pipeline(final)
        self.multi_file.intermediate_provider.needed.assert_not_called()

    def test_from_cache_with_missing_intermediate_dbs(self) -> None:
        self.options = PipelineOptions(workspace_directory=self.workspace.path, from_cache=True)
        self.multi_file.options = self.options
        self.multi_file.intermediate_provider.needed = Mock()

        self.mock_input("v2")
        self.mock_db("v2")
        self.mock_input("v3")

        intermediates, final = self.multi_file.prepare()

        self.assertEqual(len(intermediates), 1)
        self.check_intermediate_pipeline(intermediates[0], "v3")
        self.check_final_pipeline(final)
        self.multi_file.intermediate_provider.needed.assert_not_called()

    def test_from_cache_with_missing_additional_resources(self) -> None:
        self.options = PipelineOptions(workspace_directory=self.workspace.path, from_cache=True)
        self.multi_file.options = self.options
        self.multi_file.additional_resources = {"foo.txt": MockResource()}
        self.multi_file.intermediate_provider.needed = Mock()

        self.mock_input("v2")
        self.mock_db("v2")
        self.mock_input("v3")
        self.mock_db("v3")

        with self.assertRaises(MultipleDataErrors) as c:
            self.multi_file.prepare()

        self.assertEqual(len(c.exception.errors), 1)
        self.assertIsInstance(c.exception.errors[0], ResourceNotCached)

    def test_from_cache_and_force_run(self) -> None:
        self.options = PipelineOptions(
            workspace_directory=self.workspace.path,
            from_cache=True,
            force_run=True,
        )
        self.multi_file.options = self.options
        self.multi_file.intermediate_provider.needed = Mock()

        self.mock_input("v2")
        self.mock_db("v2")
        self.mock_input("v3")
        self.mock_db("v3")

        intermediates, final = self.multi_file.prepare()

        self.assertEqual(len(intermediates), 2)
        self.check_intermediate_pipeline(intermediates[0], "v2")
        self.check_intermediate_pipeline(intermediates[1], "v3")
        self.check_final_pipeline(final)
        self.multi_file.intermediate_provider.needed.assert_not_called()

    def test_saves_db_in_workspace(self) -> None:
        self.options = PipelineOptions(
            workspace_directory=self.workspace.path,
        )
        self.multi_file.options = self.options
        intermediates, final = self.multi_file.prepare()

        self.assertEqual(len(intermediates), 2)
        self.check_intermediate_pipeline(intermediates[0], "v2")
        self.check_intermediate_pipeline(intermediates[1], "v3")
        self.check_final_pipeline(final, has_pre_merge_dummy_tasks=False)

    def test_removes_failed_intermediate_dbs(self) -> None:
        self.multi_file.intermediate_pipeline_tasks_factory = mock_broken_task_factory
        pipelines = self.multi_file.prepare()

        with self.assertRaises(ValueError):
            pipelines.run()

        self.assertListEqual(list(self.multi_file.intermediate_dbs_path().iterdir()), [])
