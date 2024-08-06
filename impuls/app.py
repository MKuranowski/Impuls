from abc import ABC, abstractmethod
from argparse import ArgumentParser, Namespace
from pathlib import Path
from typing import final

from .multi_file import MultiFile
from .options import PipelineOptions
from .pipeline import Pipeline
from .resource import Resource
from .tools.logs import initialize as initialize_logging


class App(ABC):
    """App is a helper abstract class for writing applications/scripts using Impuls.
    It provides a helper, glue code from main to running a :py:class:`~impuls.Pipeline`
    or :py:class:`~impuls.multi_file.Pipelines`
    (returned by :py:class:`~impuls.multi_file.MultiFile`)::

        class MyApp(impuls.App):
            def prepare(
                self,
                args: argparse.Namespace,
                options: impuls.PipelineOptions,
            ) -> impuls.Pipeline | impuls.multi_file.MultiFile[impuls.Resource]:
                ... # Prepare your own Pipeline or MultiFile

        if __name__ == "__main__":
            MyApp().run()
    """

    name: str
    workspace_directory: Path

    def __init__(
        self,
        name: str | None = None,
        workspace_directory: Path = Path("_impuls_workspace"),
    ) -> None:
        self.name = type(self).__name__ if name is None else name
        self.workspace_directory = workspace_directory

    @abstractmethod
    def prepare(self, args: Namespace, options: PipelineOptions) -> Pipeline | MultiFile[Resource]:
        """prepare must be overwritten and must return a :py:class:`~impuls.Pipeline` or a
        :py:class:`~impuls.multi_file.MultiFile` to be run by the App.
        """
        raise NotImplementedError

    def add_arguments(self, parser: ArgumentParser) -> None:
        """add_argument may be overwritten to add extra arguments to be parsed from
        the command line. Those arguments will then be provided to the :py:meth:`~impuls.App.run`
        method.

        Several default arguments are always added, namely:

        * ``-f`` / ``--force-run``,
        * ``-c`` / ``--from-cache``,
        * ``-v`` / ``--verbose``.

        The first two options are used to create :py:class:`~impuls.PipelineOptions`,
        while the last one is used when setting up logging.
        """
        pass  # Default to no extra arguments

    def before_run(self) -> None:
        """before_run may be overwritten to execute arbitrary actions after
        :py:meth:`~impuls.App.prepare` is called, but before the Pipeline(s) are run.
        Default is to do nothing.
        """
        pass

    def after_run(self) -> None:
        """after_run may be overwritten to execute arbitrary actions after the Pipeline(s)
        are run. Default is to do nothing.
        """
        pass

    @final
    def _get_arg_parser_with_default_options(self) -> ArgumentParser:
        parser = ArgumentParser(prog=self.name)
        parser.add_argument(
            "-f",
            "--force-run",
            action="store_true",
            help="force the pipeline to run, ignoring InputNotModified",
        )
        parser.add_argument(
            "-c",
            "--from-cache",
            action="store_true",
            help="never download external resources, only using cached versions",
        )
        parser.add_argument(
            "-v",
            "--verbose",
            action="store_true",
            help="show DEBUG logging messages",
        )
        return parser

    @final
    def _parse_args(self, args_str: list[str] | None = None) -> tuple[Namespace, PipelineOptions]:
        parser = self._get_arg_parser_with_default_options()
        self.add_arguments(parser)
        args = parser.parse_args(args_str)
        options = PipelineOptions(
            force_run=args.force_run,
            from_cache=args.from_cache,
            workspace_directory=self.workspace_directory,
        )
        return args, options

    @final
    def run(self, args_str: list[str] | None = None) -> None:
        """run parses command-line arguments (either from the provided list or sys.argv),
        prepares the Pipeline(s) and runs them.
        """
        args, options = self._parse_args(args_str)
        initialize_logging(verbose=args.verbose)
        to_run = self.prepare(args, options)

        self.before_run()
        if isinstance(to_run, MultiFile):
            to_run.prepare().run()
        else:
            to_run.run()
        self.after_run()
