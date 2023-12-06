from argparse import ArgumentParser
from pathlib import Path

from impuls import HTTPResource, Pipeline, PipelineOptions, initialize_logging
from impuls.tasks import ExecuteSQL, LoadGTFS

arg_parser = ArgumentParser()
arg_parser.add_argument("type", choices=["bus", "tram"])
args = arg_parser.parse_args()

if args.type == "tram":
    source_name = "krakow.tram.zip"
    source_url = "http://gtfs.ztp.krakow.pl/GTFS_KRK_T.zip"
else:
    source_name = "krakow.bus.zip"
    source_url = "http://gtfs.ztp.krakow.pl/GTFS_KRK_A.zip"


initialize_logging(verbose=True)
Pipeline(
    options=PipelineOptions(
        force_run=True,
        save_db_in_workspace=True,
        workspace_directory=Path("_workspace_krakow"),
    ),
    tasks=[
        LoadGTFS(source_name),
        ExecuteSQL(statement="UPDATE trips SET block_id = NULL", task_name="Drop block_id"),
    ],
    resources={
        source_name: HTTPResource.get(source_url),
    },
).run()
