from pathlib import Path

from impuls import HTTPResource, Pipeline, PipelineOptions, initialize_logging
from impuls.model import Agency, Date
from impuls.tasks import AddEntity, ExecuteSQL, LoadBusManMDB, ModifyStopsFromCSV, SaveDB

from .generate_calendars import GenerateCalendars
from .stops_resource import RadomStopsResource
from .zip_resource import ZippedResource

initialize_logging(verbose=False)
Pipeline(
    options=PipelineOptions(
        force_run=True,
        workspace_directory=Path("_workspace_radom"),
    ),
    resources={
        "radom.mdb": ZippedResource(
            HTTPResource.get("http://mzdik.pl/upload/file/Rozklady-2023-11-25.zip")
        ),
        "soap_stops.csv": RadomStopsResource(),
    },
    tasks=[
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
        LoadBusManMDB("radom.mdb", agency_id="0", ignore_route_id=True, ignore_stop_id=False),
        ExecuteSQL(
            task_name="RemoveUnknownStops",
            statement=(
                "DELETE FROM stops WHERE stop_id IN ("
                "    '1220', '1221', '1222', '1223', '1224', '1225', '1226', '1227', '1228', "
                "    '1229', '649', '652', '653', '659', '662'"
                ")"
            ),
        ),
        ExecuteSQL(
            task_name="RetainKnownCalendars",
            statement=(
                "DELETE FROM calendars WHERE desc NOT IN ('POWSZEDNI', 'SOBOTA', 'NIEDZIELA')"
            ),
        ),
        GenerateCalendars(Date.today()),
        ModifyStopsFromCSV("soap_stops.csv"),
        SaveDB(Path("_workspace_radom", "impuls.db")),
    ],
).run()
