from argparse import Namespace
from pathlib import Path

from impuls import App, HTTPResource, PipelineOptions, model
from impuls.multi_file import MultiFile
from impuls.tasks import AddEntity, RemoveUnusedEntities, SaveGTFS

from .fix_stop_locations import FixStopLocations
from .generate_trip_headsign import GenerateTripHeadsign
from .import_ztm import ImportZTM
from .merge_railway_stations import MergeRailwayStations
from .remove_stops_without_locations import RemoveStopsWithoutLocations
from .ztm_ftp import FTPResource, ZTMFeedProvider

GTFS_HEADERS = {
    "agency": (
        "agency_id.txt",
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
        "zone_id",
        "wheelchair_boarding",
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
        "wheelchair_accessible",
        "exceptional",
    ),
    "stop_times.txt": (
        "trip_id",
        "stop_sequence",
        "stop_id",
        "arrival_time",
        "departure_time",
        "pickup_type",
        "drop_off_type",
    ),
    "calendar_dates.txt": ("service_id", "date", "exception_type"),
}


class WarsawGTFS(App):
    def prepare(self, args: Namespace, options: PipelineOptions) -> MultiFile[FTPResource]:
        return MultiFile(
            options=options,
            intermediate_provider=ZTMFeedProvider(),
            intermediate_pipeline_tasks_factory=lambda feed: [
                ImportZTM(
                    feed.resource_name,
                    compressed=True,
                    stop_names_resource="stop_names.json",
                ),
                AddEntity(
                    model.FeedInfo(
                        publisher_name="Mikołaj Kuranowski",
                        publisher_url="https://mkuran.pl/gtfs/",
                        lang="pl",
                        version=feed.version,
                    ),
                ),
                MergeRailwayStations(),
                FixStopLocations("stop_locations.json"),
                GenerateTripHeadsign(),
                RemoveStopsWithoutLocations(),
                RemoveUnusedEntities(),
            ],
            final_pipeline_tasks_factory=lambda _: [
                SaveGTFS(GTFS_HEADERS, options.workspace_directory / "warsaw.zip"),
            ],
            additional_resources={
                "stop_names.json": HTTPResource.get(
                    "https://raw.githubusercontent.com/MKuranowski/WarsawGTFS/master/data_curated/stop_names.json"  # noqa: E501
                ),
                "stop_locations.json": HTTPResource.get(
                    "https://raw.githubusercontent.com/MKuranowski/WarsawGTFS/master/data_curated/missing_stop_locations.json"  # noqa: E501
                ),
            },
        )


if __name__ == "__main__":
    WarsawGTFS(workspace_directory=Path("_workspace_warsaw")).run()
