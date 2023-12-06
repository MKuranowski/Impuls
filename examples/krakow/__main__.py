from argparse import ArgumentParser
from pathlib import Path

from impuls import HTTPResource, Pipeline, PipelineOptions, initialize_logging
from impuls.tasks import ExecuteSQL, LoadGTFS, RemoveUnusedEntities

from .generate_route_long_name import GenerateRouteLongName

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
        ExecuteSQL(task_name="DropBlockID", statement="UPDATE trips SET block_id = NULL"),
        ExecuteSQL(
            task_name="RemoveTripsWithoutPickup",
            statement=(
                "DELETE FROM trips WHERE NOT EXISTS (SELECT * FROM stop_times WHERE"
                "  trips.trip_id = stop_times.trip_id AND pickup_type != 1)"
            ),
        ),
        RemoveUnusedEntities(),
        ExecuteSQL(
            task_name="FixAgency",
            statement=(
                "UPDATE agencies SET name = CASE "
                "  WHEN url LIKE '%mpk.krakow.pl%' THEN 'MPK Kraków' "
                "  WHEN url LIKE '%ztp.krakow.pl%' THEN 'ZTP Kraków' "
                "  ELSE name "
                "END"
            ),
        ),
        ExecuteSQL(
            task_name="FixStopNames",
            statement=r"UPDATE stops SET name = re_sub('(\w)\.(\w)', '\1. \2', name)",
        ),
        ExecuteSQL(
            task_name="FixTripHeadsign",
            statement=(
                "UPDATE trips SET headsign = "
                r"re_sub(' *\(n[zż]\)$', '', re_sub('(\w)\.(\w)', '\1. \2', headsign))"
            ),
        ),
        ExecuteSQL(
            task_name="FixRouteColor",
            statement=(
                "UPDATE routes SET text_color = 'FFFFFF', color ="
                "  CASE type"
                "    WHEN 0 THEN '002E5F'"
                "    ELSE '0072AA'"
                "  END"
            ),
        ),
        ExecuteSQL(
            task_name="GenerateStopCode",
            statement=(
                "UPDATE stops SET code ="
                "  CASE"
                # Tram stops: last 2 digits 'x9' map to 0x
                "    WHEN substr(stop_id, -2, 2) GLOB '[1-9]9' THEN '0' || substr(stop_id, -2, 1)"
                # Default: last two digits of the stop_id are the stop_code
                "    WHEN substr(stop_id, -2, 2) GLOB '[0-9][0-9]' THEN substr(stop_id, -2, 2)"
                "    ELSE ''"
                "  END"
            ),
        ),
        GenerateRouteLongName(),
    ],
    resources={
        source_name: HTTPResource.get(source_url),
    },
).run()
