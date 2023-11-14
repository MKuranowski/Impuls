from pathlib import Path

from impuls import HTTPResource, Pipeline, PipelineOptions, initialize_logging
from impuls.tasks import SaveGTFS

from .import_ztm import ImportZTM
from .merge_railway_stations import MergeRailwayStations
from .ztm_ftp import FTPResource

GTFS_HEADERS = {
    "agency": (
        "agency_id",
        "agency_name",
        "agency_url",
        "agency_timezone",
        "agency_lang",
        "agency_phone",
    ),
    "stops": (
        "stop_id",
        "stop_name",
        "stop_lat",
        "stop_lon",
        "zone_id",
        "wheelchair_boarding",
    ),
    "routes": (
        "agency_id",
        "route_id",
        "route_short_name",
        "route_long_name",
        "route_type",
        "route_color",
        "route_text_color",
    ),
    "trips": (
        "route_id",
        "service_id",
        "trip_id",
        "trip_headsign",
        "direction_id",
        "wheelchair_accessible",
        "exceptional",
    ),
    "stop_times": (
        "trip_id",
        "stop_sequence",
        "stop_id",
        "arrival_time",
        "departure_time",
        "pickup_type",
        "drop_off_type",
    ),
    "calendar_dates": ("service_id", "date", "exception_type"),
}

initialize_logging(verbose=True)
Pipeline(
    tasks=[
        ImportZTM("ztm.7z", compressed=True, stop_names_resource="stop_names.json"),
        MergeRailwayStations(),
        SaveGTFS(GTFS_HEADERS, Path("_workspace_warsaw/warsaw.zip")),
    ],
    resources={
        "ztm.7z": FTPResource("RA231111.7z"),
        "stop_names.json": HTTPResource.get(
            "https://raw.githubusercontent.com/MKuranowski/WarsawGTFS/master/data_curated/stop_names.json"  # noqa: E501
        ),
    },
    options=PipelineOptions(
        force_run=True,
        workspace_directory=Path("_workspace_warsaw"),
    ),
).run()
