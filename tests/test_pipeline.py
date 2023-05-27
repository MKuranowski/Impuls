import json
from datetime import datetime
from typing import final
from unittest import TestCase

from impuls import Pipeline, PipelineOptions, Task, TaskRuntime
from impuls.errors import InputNotModified
from impuls.tools.testing_mocks import MockFile, MockResource


@final
class DummyTask(Task):
    def __init__(self, name: str | None = None) -> None:
        super().__init__(name)
        self.executed_count = 0

    def execute(self, r: TaskRuntime) -> None:
        self.executed_count += 1


class TestPipeline(TestCase):
    def setUp(self) -> None:
        self.workspace_dir = MockFile(directory=True)

    def tearDown(self) -> None:
        self.workspace_dir.cleanup()

    def test_executes_tasks(self) -> None:
        t1 = DummyTask("DummyTask1")
        t2 = DummyTask("DummyTask2")
        p = Pipeline([t1, t2])
        with p:
            p.run()

        self.assertEqual(t1.executed_count, 1)
        self.assertEqual(t2.executed_count, 1)

    def test_fetches_resources(self) -> None:
        @final
        class ResourceCheckTask(Task):
            def __init__(self, test: "TestPipeline") -> None:
                self.called = False
                self.test = test
                super().__init__()

            def execute(self, r: TaskRuntime) -> None:
                self.called = True
                self.test.assertEqual(len(r.resources), 2)
                self.test.assertIn("hello.txt", r.resources)
                self.test.assertIn("lorem.dat", r.resources)

                res = r.resources["hello.txt"]
                self.test.assertEqual(res.stored_at, self.test.workspace_dir.path / "hello.txt")
                self.test.assertEqual(res.text(), "Hello, world!\n")

                res = r.resources["lorem.dat"]
                self.test.assertEqual(res.stored_at, self.test.workspace_dir.path / "lorem.dat")
                self.test.assertEqual(res.bytes(), b"Lorem ipsum dolor sit\n")

        t = ResourceCheckTask(self)
        p = Pipeline(
            tasks=[t],
            resources={
                "hello.txt": MockResource(b"Hello, world!\n"),
                "lorem.dat": MockResource(b"Lorem ipsum dolor sit\n"),
            },
            options=PipelineOptions(workspace_directory=self.workspace_dir.path),
        )
        with p:
            p.run()

        self.assertTrue(t.called)

    def test_raises_input_not_modified(self) -> None:
        # Pretend the resource is cached
        self.workspace_dir.path.joinpath("hello.txt").write_bytes(b"Hello, world!\n")
        with self.workspace_dir.path.joinpath("hello.txt.metadata").open(mode="w") as f:
            json.dump(
                {
                    "last_modified": datetime.fromisoformat("2023-04-01T11:30:00Z").timestamp(),
                    "fetch_time": datetime.fromisoformat("2023-04-01T12:00:00Z").timestamp(),
                },
                f,
            )

        p = Pipeline(
            tasks=[DummyTask()],
            resources={"hello.txt": MockResource(b"Hello, world!\n")},
            options=PipelineOptions(workspace_directory=self.workspace_dir.path),
        )
        with p, self.assertRaises(InputNotModified):
            p.run()

    def test_renames_task_loggers(self) -> None:
        o = PipelineOptions(workspace_directory=self.workspace_dir.path)

        foo = DummyTask("Foo")
        bar = DummyTask("Bar")

        p = Pipeline(tasks=[foo, bar], options=o)
        self.assertEqual(p.name, "")
        self.assertEqual(foo.name, "Foo")
        self.assertEqual(foo.logger.name, "Task.Foo")
        self.assertEqual(bar.name, "Bar")
        self.assertEqual(bar.logger.name, "Task.Bar")

        p = Pipeline(tasks=[foo, bar], options=o, name="Eggs")
        self.assertEqual(p.name, "Eggs")
        self.assertEqual(foo.name, "Foo")
        self.assertEqual(foo.logger.name, "Eggs.Task.Foo")
        self.assertEqual(bar.name, "Bar")
        self.assertEqual(bar.logger.name, "Eggs.Task.Bar")

    def test_option_force_run(self) -> None:
        @final
        class ResourceCheckTask(Task):
            def __init__(self, test: "TestPipeline") -> None:
                self.called = False
                self.test = test
                super().__init__()

            def execute(self, r: TaskRuntime) -> None:
                self.called = True
                self.test.assertEqual(len(r.resources), 1)
                self.test.assertIn("hello.txt", r.resources)

                res = r.resources["hello.txt"]
                self.test.assertEqual(res.stored_at, self.test.workspace_dir.path / "hello.txt")
                self.test.assertEqual(res.text(), "Hello, world!\n")

        # Pretend the resource is cached
        self.workspace_dir.path.joinpath("hello.txt").write_bytes(b"Hello, world!\n")
        with self.workspace_dir.path.joinpath("hello.txt.metadata").open(mode="w") as f:
            json.dump(
                {
                    "last_modified": datetime.fromisoformat("2023-04-01T11:30:00Z").timestamp(),
                    "fetch_time": datetime.fromisoformat("2023-04-01T12:00:00Z").timestamp(),
                },
                f,
            )

        t = ResourceCheckTask(self)
        p = Pipeline(
            tasks=[t],
            resources={"hello.txt": MockResource(b"Hello, world!\n")},
            options=PipelineOptions(workspace_directory=self.workspace_dir.path, force_run=True),
        )

        # NOTE: As opposed to test_raises_input_not_modified, the following
        #       must not raise InputNotModified.
        with p:
            p.run()

        self.assertTrue(t.called)

    def test_option_from_cache(self) -> None:
        @final
        class ResourceCheckTask(Task):
            def __init__(self, test: "TestPipeline") -> None:
                self.called = False
                self.test = test
                super().__init__()

            def execute(self, r: TaskRuntime) -> None:
                self.called = True
                self.test.assertEqual(len(r.resources), 1)
                self.test.assertIn("hello.txt", r.resources)

                res = r.resources["hello.txt"]
                # NOTE: from_cache should not fetch the resource
                self.test.assertEqual(res.stored_at, self.test.workspace_dir.path / "hello.txt")
                self.test.assertEqual(res.text(), "Hello, world!\n")
                self.test.assertEqual(
                    res.last_modified,
                    datetime.fromisoformat("2023-04-01T11:30:00Z"),
                )
                self.test.assertEqual(
                    res.fetch_time,
                    datetime.fromisoformat("2023-04-01T12:00:00Z"),
                )

        # Pretend the resource is cached
        self.workspace_dir.path.joinpath("hello.txt").write_bytes(b"Hello, world!\n")
        with self.workspace_dir.path.joinpath("hello.txt.metadata").open(mode="w") as f:
            json.dump(
                {
                    "last_modified": datetime.fromisoformat("2023-04-01T11:30:00Z").timestamp(),
                    "fetch_time": datetime.fromisoformat("2023-04-01T12:00:00Z").timestamp(),
                },
                f,
            )

        # Pretend a newer version is available
        r = MockResource(
            b"Hello, new world!\n",
            last_modified=datetime.fromisoformat("2023-05-01T11:00:00Z"),
            persist_last_modified=True,
        )

        t = ResourceCheckTask(self)
        p = Pipeline(
            tasks=[t],
            resources={"hello.txt": r},
            options=PipelineOptions(workspace_directory=self.workspace_dir.path, from_cache=True),
        )

        # NOTE: As opposed to test_raises_input_not_modified, the following
        #       must not raise InputNotModified.
        with p:
            p.run()

        self.assertTrue(t.called)

    def test_option_save_db_in_workspace(self) -> None:
        o = PipelineOptions(save_db_in_workspace=True, workspace_directory=self.workspace_dir.path)
        p = Pipeline([DummyTask()], options=o)
        with p:
            p.run()
        self.assertTrue(self.workspace_dir.path.joinpath("impuls.db").exists())
