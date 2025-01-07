import logging
from argparse import Namespace
from pathlib import Path

from impuls import App, PipelineOptions
from impuls.model import Agency, FeedInfo
from impuls.multi_file import MultiFile
from impuls.resource import ZippedResource
from impuls.tasks import AddEntity, ExecuteSQL, LoadBusManMDB, ModifyStopsFromCSV, SaveGTFS
from impuls.tools import polish_calendar_exceptions

from .generate_calendars import GenerateCalendars
from .provider import RadomProvider
from .stops_resource import RadomStopsResource

GTFS_HEADERS = {
    "agency.txt": (
        "agency_id",
        "agency_name",
        "agency_url",
        "agency_timezone",
        "agency_lang",
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
    ),
    "trips.txt": (
        "route_id",
        "service_id",
        "trip_id",
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
        "service_desc",
    ),
    "calendar_dates.txt": ("service_id", "date", "exception_type"),
}


class RadomGTFS(App):
    def before_run(self) -> None:
        logging.getLogger("zeep").setLevel(logging.INFO)

    def prepare(self, args: Namespace, options: PipelineOptions) -> MultiFile[ZippedResource]:
        return MultiFile(
            options=options,
            intermediate_provider=RadomProvider(),
            intermediate_pipeline_tasks_factory=lambda feed: [
                AddEntity(
                    Agency(
                        id="0",
                        name="MZDiK Radom",
                        url="http://www.mzdik.radom.pl/",
                        timezone="Europe/Warsaw",
                        lang="pl",
                    ),
                    task_name="AddAgency",
                ),
                AddEntity(
                    FeedInfo(
                        publisher_name="Mikołaj Kuranowski",
                        publisher_url="https://mkuran.pl/gtfs/",
                        lang="pl",
                        version=feed.version,
                    ),
                    task_name="AddFeedInfo",
                ),
                LoadBusManMDB(
                    feed.resource_name,
                    agency_id="0",
                    ignore_route_id=True,
                    ignore_stop_id=False,
                ),
                ExecuteSQL(
                    task_name="RemoveUnknownStops",
                    statement=(
                        "DELETE FROM stops WHERE stop_id IN ("
                        "    '1220', '1221', '1222', '1223', '1224', '1225', '1226', '1227', "
                        "    '1228', '1229', '649', '652', '653', '659', '662'"
                        ")"
                    ),
                ),
                ExecuteSQL(
                    task_name="RetainKnownCalendars",
                    statement=(
                        "DELETE FROM calendars WHERE desc NOT IN "
                        "('POWSZEDNI', 'SOBOTA', 'NIEDZIELA')"
                    ),
                ),
                GenerateCalendars(feed.start_date),
                ModifyStopsFromCSV("soap_stops.csv"),
            ],
            final_pipeline_tasks_factory=lambda _: [
                SaveGTFS(GTFS_HEADERS, options.workspace_directory / "radom.zip"),
            ],
            additional_resources={
                "soap_stops.csv": RadomStopsResource(),
                "calendar_exceptions.csv": polish_calendar_exceptions.RESOURCE,
            },
        )


if __name__ == "__main__":
    RadomGTFS(workspace_directory=Path("_workspace_radom")).run()
