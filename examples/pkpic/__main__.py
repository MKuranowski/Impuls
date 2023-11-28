from argparse import ArgumentParser
from pathlib import Path

from impuls import Pipeline, PipelineOptions, initialize_logging
from impuls.model import Agency
from impuls.resource import HTTPResource, ZippedResource
from impuls.tasks import AddEntity

from .csv_import import CSVImport
from .ftp_resource import FTPResource
from .station_import import ImportStationData

arg_parser = ArgumentParser()
arg_parser.add_argument("username", help="ftps.intercity.pl username")
arg_parser.add_argument("password", help="ftps.intercity.pl password")
args = arg_parser.parse_args()

initialize_logging(verbose=False)
Pipeline(
    options=PipelineOptions(
        force_run=True,
        workspace_directory=Path("_workspace_pkpic"),
        save_db_in_workspace=True,
    ),
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
).run()
