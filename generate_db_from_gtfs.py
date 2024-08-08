# © Copyright 2022-2024 Mikołaj Kuranowski
# SPDX-License-Identifier: GPL-3.0-or-later

import argparse
from pathlib import Path
from tempfile import TemporaryDirectory

import impuls
import impuls.tasks


def generate_db_from_gtfs(gtfs: Path, output: Path) -> None:
    with TemporaryDirectory() as temp_workspace_str:
        temp_workspace = Path(temp_workspace_str)
        pipeline = impuls.Pipeline(
            tasks=[
                impuls.tasks.LoadGTFS(gtfs.name),
                impuls.tasks.SaveDB(output),
            ],
            resources={gtfs.name: impuls.LocalResource(gtfs)},
            options=impuls.PipelineOptions(
                force_run=True,
                workspace_directory=temp_workspace,
            ),
        )
        pipeline.run()


def main() -> None:
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument(
        "-o",
        "--output",
        help="path to output file",
        type=Path,
        default="impuls.db",
    )
    arg_parser.add_argument("gtfs", help="path to GTFS", metavar="FILE", type=Path)
    args = arg_parser.parse_args()

    impuls.initialize_logging(verbose=True)
    generate_db_from_gtfs(args.gtfs, args.output)


if __name__ == "__main__":
    main()
