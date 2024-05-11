import argparse
from pathlib import Path
from shutil import copy
from tempfile import TemporaryDirectory

import impuls
import impuls.tasks


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
    gtfs: Path = args.gtfs
    output: Path = args.output

    with TemporaryDirectory() as temp_workspace_str:
        temp_workspace = Path(temp_workspace_str)
        pipeline = impuls.Pipeline(
            tasks=[impuls.tasks.LoadGTFS(gtfs.name)],
            resources={gtfs.name: impuls.LocalResource(gtfs)},
            options=impuls.PipelineOptions(
                force_run=True,
                workspace_directory=temp_workspace,
            ),
        )
        impuls.initialize_logging(verbose=True)
        pipeline.run()

        copy(temp_workspace / "impuls.db", output)


if __name__ == "__main__":
    main()
