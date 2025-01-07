from argparse import ArgumentParser, Namespace
from pathlib import Path

from impuls import App, HTTPResource, Pipeline, PipelineOptions
from impuls.tasks import ExecuteSQL, LoadGTFS, RemoveUnusedEntities, SaveGTFS

from .generate_route_long_name import GenerateRouteLongName

GTFS_HEADERS = {
    "agency.txt": (
        "agency_id",
        "agency_name",
        "agency_url",
        "agency_timezone",
        "agency_lang",
        "agency_phone",
    ),
    "stops.txt": (
        "stop_id",
        "stop_code",
        "stop_name",
        "stop_lat",
        "stop_lon",
    ),
    "routes.txt": (
        "agency_id",
        "route_id",
        "route_short_name",
        "route_long_name",
        "route_type",
        "route_color",
        "route_text_color",
    ),
    "trips.txt": (
        "route_id",
        "service_id",
        "trip_id",
        "trip_headsign",
        "direction_id",
    ),
    "stop_times.txt": (
        "trip_id",
        "stop_sequence",
        "stop_id",
        "arrival_time",
        "departure_time",
    ),
    "calendar.txt": (
        "service_id",
        "start_date",
        "end_date",
        "monday",
        "tuesday",
        "wednesday",
        "thursday",
        "friday",
        "saturday",
        "sunday",
    ),
    "calendar_dates.txt": (
        "service_id",
        "date",
        "exception_type",
    ),
}


class KrakowGTFS(App):
    def add_arguments(self, parser: ArgumentParser) -> None:
        parser.add_argument("type", choices=["bus", "tram"])

    @staticmethod
    def get_source_name_and_url(typ: str) -> tuple[str, str]:
        if typ == "tram":
            return "krakow.tram.zip", "http://gtfs.ztp.krakow.pl/GTFS_KRK_T.zip"
        else:
            return "krakow.bus.zip", "http://gtfs.ztp.krakow.pl/GTFS_KRK_A.zip"

    def prepare(self, args: Namespace, options: PipelineOptions) -> Pipeline:
        source_name, source_url = self.get_source_name_and_url(args.type)
        return Pipeline(
            options=options,
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
                        "    WHEN substr(stop_id, -2, 2) GLOB '[1-9]9'"
                        "      THEN '0' || substr(stop_id, -2, 1)"
                        # Default: last two digits of the stop_id are the stop_code
                        "    WHEN substr(stop_id, -2, 2) GLOB '[0-9][0-9]'"
                        "      THEN substr(stop_id, -2, 2)"
                        "    ELSE ''"
                        "  END"
                    ),
                ),
                GenerateRouteLongName(),
                SaveGTFS(
                    GTFS_HEADERS,
                    options.workspace_directory / f"krakow.{args.type}.out.zip",
                ),
            ],
            resources={
                source_name: HTTPResource.get(source_url),
            },
        )


if __name__ == "__main__":
    KrakowGTFS(workspace_directory=Path("_workspace_krakow")).run()
