from argparse import ArgumentParser, Namespace
from pathlib import Path

from impuls import App, Pipeline, PipelineOptions
from impuls.model import Agency
from impuls.resource import HTTPResource, ZippedResource
from impuls.tasks import AddEntity, GenerateTripHeadsign, SaveGTFS

from .csv_import import CSVImport
from .ftp_resource import FTPResource
from .set_colors import SetRouteColors
from .split_bus_legs import SplitBusLegs
from .station_import import ImportStationData

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
        "trip_short_name",
    ),
    "stop_times.txt": (
        "trip_id",
        "stop_sequence",
        "stop_id",
        "arrival_time",
        "departure_time",
        "platform",
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
}


class PKPIntercityGTFS(App):
    def add_arguments(self, parser: ArgumentParser) -> None:
        parser.add_argument("username", help="ftps.intercity.pl username")
        parser.add_argument("password", help="ftps.intercity.pl password")

    def prepare(self, args: Namespace, options: PipelineOptions) -> Pipeline:
        return Pipeline(
            options=options,
            tasks=[
                AddEntity(
                    Agency(
                        id="0",
                        name="PKP Intercity",
                        url="https://intercity.pl",
                        timezone="Europe/Warsaw",
                        lang="pl",
                        phone="+48703200200",
                    ),
                    task_name="AddAgency",
                ),
                CSVImport("rozklad_kpd.csv"),
                ImportStationData("pl_rail_map.osm"),
                GenerateTripHeadsign(),
                SplitBusLegs(),
                SetRouteColors(),
                SaveGTFS(GTFS_HEADERS, options.workspace_directory / "pkpic.zip"),
            ],
            resources={
                "rozklad_kpd.csv": ZippedResource(
                    FTPResource("rozklad/KPD_Rozklad.zip", args.username, args.password),
                    file_name_in_zip="KPD_Rozklad.csv",
                ),
                "pl_rail_map.osm": HTTPResource.get(
                    "https://raw.githubusercontent.com/MKuranowski/PLRailMap/master/plrailmap.osm",
                ),
            },
        )


if __name__ == "__main__":
    PKPIntercityGTFS(workspace_directory=Path("_workspace_pkpic")).run()
