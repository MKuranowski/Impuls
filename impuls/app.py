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
    def __init__(self, name: str, workspace_directory: Path = Path("_impuls_workspace")) -> None:
        self.name = name
        self.workspace_directory = workspace_directory

    @abstractmethod
    def prepare(self, args: Namespace, options: PipelineOptions) -> Pipeline | MultiFile[Resource]:
        raise NotImplementedError

    def add_arguments(self, parser: ArgumentParser) -> None:
        pass  # Default to no extra arguments

    def before_run(self) -> None:
        pass

    def after_run(self) -> None:
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
            "-s",
            "--save-db",
            action="store_true",
            help="work on a database saved in the workspace, instead of a in-memory one",
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
            save_db_in_workspace=args.save_db,
        )
        return args, options

    @final
    def run(self, args_str: list[str] | None = None) -> None:
        args, options = self._parse_args(args_str)
        initialize_logging(verbose=args.verbose)
        to_run = self.prepare(args, options)

        self.before_run()
        if isinstance(to_run, MultiFile):
            to_run.prepare().run()
        else:
            to_run.run()
        self.after_run()
