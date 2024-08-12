from argparse import ArgumentParser, Namespace
from io import StringIO
from unittest import TestCase
from unittest.mock import patch

from impuls import App, Pipeline, PipelineOptions, Task, TaskRuntime
from impuls.errors import InputNotModified


class DummyTask(Task):
    def __init__(self) -> None:
        super().__init__()
        self.executed_count = 0

    def execute(self, r: TaskRuntime) -> None:
        self.executed_count += 1


class ExceptionRaisingTask(Task):
    def __init__(self, e: Exception) -> None:
        super().__init__()
        self.e = e

    def execute(self, r: TaskRuntime) -> None:
        raise self.e


class TestApp(TestCase):
    def test(self) -> None:
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

    def test_passes_exceptions(self) -> None:
        class DummyApp(App):
            def prepare(self, args: Namespace, options: PipelineOptions) -> Pipeline:
                return Pipeline([ExceptionRaisingTask(ValueError("foo"))], options=options)

        app = DummyApp()
        with self.assertRaises(ValueError):
            app.run([])

    def test_input_not_modified(self) -> None:
        class DummyApp(App):
            def prepare(self, args: Namespace, options: PipelineOptions) -> Pipeline:
                return Pipeline([ExceptionRaisingTask(InputNotModified())], options=options)

        app = DummyApp()
        stderr = StringIO()
        with patch("sys.stderr", stderr), self.assertRaises(SystemExit) as c:
            app.run([])

        self.assertEqual(c.exception.code, 2)

        stderr_content = stderr.getvalue()
        self.assertIn("Traceback", stderr_content)
        self.assertIn("InputNotModified", stderr_content)

    def test_input_not_modified_custom_code(self) -> None:
        class DummyApp(App):
            def prepare(self, args: Namespace, options: PipelineOptions) -> Pipeline:
                return Pipeline([ExceptionRaisingTask(InputNotModified())], options=options)

        app = DummyApp()
        with self.assertRaises(SystemExit) as c:
            app.run(["-I", "42"])
        self.assertEqual(c.exception.code, 42)
