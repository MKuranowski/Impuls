from argparse import ArgumentParser, Namespace
from typing import final
from unittest import TestCase

from impuls import App, Pipeline, PipelineOptions, Task, TaskRuntime


@final
class DummyTask(Task):
    def __init__(self) -> None:
        super().__init__()
        self.executed_count = 0

    def execute(self, r: TaskRuntime) -> None:
        self.executed_count += 1


class TestApp(TestCase):
    def test(self) -> None:
        @final
        class DummyApp(App):
            def __init__(self) -> None:
                super().__init__("DummyApp")
                self.task = DummyTask()
                self.before_run_calls = 0
                self.after_run_calls = 0

            def prepare(self, args: Namespace, options: PipelineOptions) -> Pipeline:
                return Pipeline([self.task], options=options)

            def before_run(self) -> None:
                self.before_run_calls += 1

            def after_run(self) -> None:
                self.after_run_calls += 1

        app = DummyApp()
        app.run([])
        self.assertEqual(app.task.executed_count, 1)
        self.assertEqual(app.before_run_calls, 1)
        self.assertEqual(app.after_run_calls, 1)

    def test_custom_arguments(self) -> None:
        @final
        class DummyApp(App):
            def __init__(self) -> None:
                super().__init__("DummyApp")
                self.foo = None
                self.bar = None

            def add_arguments(self, parser: ArgumentParser) -> None:
                parser.add_argument("foo", type=int)
                parser.add_argument("--bar", nargs="*")

            def prepare(self, args: Namespace, options: PipelineOptions) -> Pipeline:
                self.foo = args.foo
                self.bar = args.bar
                return Pipeline([], options=options)

        app = DummyApp()
        app.run(["42", "--bar", "a", "b"])
        self.assertEqual(app.foo, 42)
        self.assertEqual(app.bar, ["a", "b"])
