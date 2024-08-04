Example Usage
=============

Impuls contains multiple various bits and pieces to help process transit
data, and it it not entirely obvious how to use this library from the
:doc:`/api`. Following through this tutorial should give you a good idea
on how to use Impuls.

This tutorial assumes you are more-or-less familiar with `Python <https://www.python.org/>`_,
SQL and `GTFS <https://gtfs.org/>`_ and that you understand the high-level overview of Impuls from
:doc:`the main page </index>`. Going through :doc:`/db_schema` also won't hurt.

Full source code of the examples below is also included in the
`repository (in the examples directory) <https://github.com/MKuranowski/Impuls/tree/main/examples>`_.


Environment setup
-----------------

To use Impuls in your project, install it from `PyPI <https://pypi.org/project/impuls>`_.
Most basic setup with a `venv <https://docs.python.org/3/library/venv.html>`_ and
`pip <https://docs.python.org/3/installing/index.html#installing-index>`_ would look like this::

    python -m venv --upgrade-deps .venv
    source .venv/bin/activate
    pip install --upgrade pip

If you are using different tools to manage Python projects, you should already know how to install
packages through them.

Impuls uses `semantic versioning <https://semver.org/>`_ and major releases will come with breaking
changes. When defining dependencies on Impuls (e.g. through `requirements.txt <https://pip.pypa.io/en/stable/reference/requirements-file-format/>`_)
always use the `compatible release clause (~=) <https://packaging.python.org/en/latest/specifications/version-specifiers/#compatible-release>`_,
e.g. ``impuls~=1.0``, or any other way to force a specific major version of the library.


Fixing a GTFS (Kraków)
----------------------

The first example involves fixing `GTFS files for Kraków <https://gtfs.ztp.krakow.pl/>`_.
Impuls provides an entry point to its workings through the :py:class:`~impuls.App` class.
Its usage is not necessary - the main point of the library is :py:class:`~impuls.Pipeline` -
however, :py:class:`~impuls.App` provides a bit of boilerplate to connect the command line
to the data processing pipeline. Let's start by defining an empty Pipeline::

    import argparse
    import impuls

    class KrakowGTFS(impuls.App):
        def prepare(self, args: argparse.Namespace, options: impuls.PipelineOptions) -> impuls.Pipeline:
            return impuls.Pipeline(
                tasks=[],
                options=options,
            )

    if __name__ == "__main__":
        KrakowGTFS("KrakowGTFS").run()

Although this code doesn't do much - it can still be run. :py:class:`~impuls.App` automatically
sets up pretty logging and can parse some :py:class:`~impuls.PipelineOptions` from the
command line (run with ``--help`` to see).

Let's load the GTFS data. For now we'll use the tram GTFS::

    return impuls.Pipeline(
        tasks=[
            impuls.tasks.LoadGTFS("krakow.tram.zip"),
        ],
        resources={
            "krakow.tram.zip": impuls.HTTPResource.get("https://gtfs.ztp.krakow.pl/GTFS_KRK_T.zip"),
        },
        options=options,
    )

Impuls will automatically pull and cache the GTFS file, then execute the :py:class:`~impuls.tasks.LoadGTFS`
task. If you run this script twice, the second run should fail with :py:exc:`~impuls.errors.InputNotModified`.
This is by design - by default the :py:class:`~impuls.Pipeline` refuses to run if all of the inputs are the same.
This is to avoid unnecessary processing of already-processed data. Run the script with ``-f``
(``--force-run``, :py:attr:`PipelineOptions.force_run <impuls.PipelineOptions.force_run>`)
to override this behavior. While developing with Impuls, you are going to run the pipeline a lot
in a short time span. The ``-c`` (``--from-cache``, :py:attr:`PipelineOptions.from_cache <impuls.PipelineOptions.from_cache>`)
option can be very helpful, to avoid going through all of the external resources and checking
if they have changed.

The loaded data is stored temporarily in an SQLite database at ``_impuls_workspace/impuls.db``.
You can preview it with a tool like `DB Browser for SQLite <https://sqlitebrowser.org/>`_.

Loading the GTFS all by itself isn't very useful. It's now time to fix the data.
There aren't that many builtin tasks available (see :py:mod:`impuls.tasks`), but most simple
fixes can be encapsulated in the :py:class:`~impuls.tasks.ExecuteSQL` task. Writing SQL queries
directly is also the fastest way to operate on the loaded data, as objects don't have to cross
the Python-SQLite barrier, necessitating costly conversions.

Let's start by updating the agency name, route colors and removing pointless block transfers::

    tasks=[
        impuls.tasks.LoadGTFS("krakow.tram.zip"),
        impuls.tasks.ExecuteSQL(
            task_name="FixAgency",
            statement=(
                "UPDATE agencies SET name = CASE "
                "  WHEN url LIKE '%mpk.krakow.pl%' THEN 'MPK Kraków' "
                "  WHEN url LIKE '%ztp.krakow.pl%' THEN 'ZTP Kraków' "
                "  ELSE name "
                "END"
            ),
        ),
        impuls.tasks.ExecuteSQL(
            task_name="FixRouteColor",
            statement=(
                "UPDATE routes SET text_color = 'FFFFFF', color ="
                "  CASE type"
                "    WHEN 0 THEN '002E5F'"
                "    ELSE '0072AA'"
                "  END"
            ),
        ),
        impuls.tasks.ExecuteSQL(
            task_name="DropBlockID",
            statement="UPDATE trips SET block_id = NULL",
        ),
    ]

After running the pipeline with new tasks, you should see your changes in the ``impuls.db`` file.

SQL is very powerful and can do more complicated data fixes. The source data includes
depot runs in trips.txt, with all stop times set to be unavailable to passengers. Such
trips can be removed with a single nested SQL query. Even though we want to remove
trips with all pickup_type = 1 stop_times, SQLite only has an EXISTS clause, so we need
to `negate the condition <https://en.wikipedia.org/wiki/De_Morgan%27s_laws#Extension_to_predicate_and_modal_logic>`_:
remove all trips without any pickup_type ≠ 1 stop_time::

    impuls.tasks.ExecuteSQL(
        task_name="RemoveTripsWithoutPickup",
        statement=(
            "DELETE FROM trips WHERE NOT EXISTS ("
            "  SELECT * FROM stop_times WHERE"
            "  trips.trip_id = stop_times.trip_id AND pickup_type != 1
            ")"
        ),
    )

Another task requiring more complex SQL queries is extracting two-digit stop codes from stop ids.
Usually last 2 digits of a stop id are the stop code, except for tram stops where x9 id suffix
maps to 0x stop codes. We'd also want to prevent garbage stop codes if the format of stop_id
changes. All of this can be accomplished with SQLite's `substr <https://www.sqlite.org/lang_corefunc.html#substr>`_
and `GLOB <https://www.sqlite.org/lang_corefunc.html#glob>`_ functions::

    impuls.tasks.ExecuteSQL(
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
    )

Impuls makes exposes text-related functions to the SQLite interface
(see :py:class:`~impuls.DBConnection` for details). We can use `re_sub <https://docs.python.org/3/library/re.html#re.sub>`_
to fix missing spaces around dots in trip headsigns and stop names, and remove the " (nż)" unnecessary
suffix (from headsigns only)::

    impuls.tasks.ExecuteSQL(
        task_name="FixStopNames",
        statement=r"UPDATE stops SET name = re_sub('(\w)\.(\w)', '\1. \2', name)",
    )

    impuls.tasks.ExecuteSQL(
        task_name="FixTripHeadsign",
        statement=(
            "UPDATE trips SET headsign = "
            r"re_sub(' *\(n[zż]\)$', '', re_sub('(\w)\.(\w)', '\1. \2', headsign))"
        ),
    )

We're almost done! As the last thing we want to generate route long names (e.g. "Downtown - Airport")
from the most common headsigns in the outbound and inbound directions. While this is doable
with SQL only, it is difficult to deal with some edge cases, particularly when a route only
has trips in a single direction. Let's use this as an excuse to implement our own
:py:class:`~impuls.Task`. The main logic of the task is to take all of the routes
and then generate long names for them. We can start like this::

    from impuls import DBConnection, Task, TaskRuntime
    from typing import cast

    class GenerateRouteLongName(Task):
        def execute(self, r: TaskRuntime) -> None:
            with r.db.transaction():
                route_ids = [
                    cast(str, i[0])
                    for i in r.db.raw_execute("SELECT route_id FROM routes")
                ]

                r.db.raw_execute_many(
                    "UPDATE routes SET long_name = ? WHERE route_id = ?"
                    (
                        (self.generate_long_name(r.db, route_id), route_id)
                        for route_id in route_ids
                    )
                )

We'll deal with ``generate_long_name`` shortly. The main takeaway now is that implementing
tasks boils down to implementing the abstract :py:meth:`Task.execute <impuls.Task.execute>` method
and operate on the provided :py:class:`~impuls.TaskRuntime`. Tasks are not executed in parallel,
so they can safely hold some execution-related state, however be sure to clear them up on entry
to :py:meth:`~impuls.Task.execute`. When overriding ``__init__``, either to take extra parameters
or initialize internal state, be sure to call ``super().__init__()``. Tasks automatically
come with a :py:attr:`~impuls.Task.logger`. Take a look at the reference of :py:class:`~impuls.Task`
:py:class:`~impuls.TaskRuntime` and :py:class:`~impuls.DBConnection` to fully understand the
available functionality provided to tasks.

Going back to Kraków, we need to generate the route headsigns based on the most common headsigns.
To deal with the edge case of routes with a single direction, we'll generate a placeholder "Foo - Foo"
long name::

    class GenerateRouteLongName(Task):
        def generate_long_name(self, db: DBConnection, route_id: str) -> str:
            outbound = self.get_most_common_headsign(db, route_id, 0)
            inbound = self.get_most_common_headsign(db, route_id, 1)

            if outbound and inbound:
                return f"{outbound} — {inbound}"
            elif outbound:
                return f"{outbound} — {outbound}"
            elif inbound:
                return f"{inbound} — {inbound}"
            else:
                return ""

        def get_most_common_headsign(self, db: DBConnection, route_id: str, direction: int) -> str:
            result = db.raw_execute(
                "SELECT headsign FROM trips WHERE route_id = ? AND direction = ? "
                "GROUP BY headsign ORDER BY COUNT(*) DESC LIMIT 1",
                (route_id, direction),
            ).one()
            return cast(str, result[0]) if result else ""

We can now simply add ``GenerateRouteLongName()`` to the task list.

We have started by simply hard-coding the tram GTFS. We can hook into :py:class:`App's <impuls.App>`
`argument parsing <https://docs.python.org/3/library/argparse.html>`_ to select the bus/tram GTFS
based on a command line argument::

    class KrakowGTFS(impuls.App):
        def add_argument(self, parser: argparse.ArgumentParser) -> None:
            parser.add_argument("type", choices=["bus", "tram"])

        def prepare(self, args: argparse.Namespace, options: impuls.PipelineOptions) -> impuls.Pipeline:
            source_name, source_url = self.get_source_name_and_url(args.type)
            return impuls.Pipeline(
                tasks=[
                    impuls.tasks.LoadGTFS(source_name),
                    # ...
                ],
                resources={
                    source_name: impuls.HTTPResource.get(source_url),
                },
                options=options,
            )

        @staticmethod
        def get_source_name_and_url(type: str) -> tuple[str, str]:
            if type == "tram":
                return "krakow.tram.zip", "https://gtfs.ztp.krakow.pl/GTFS_KRK_T.zip"
            else:
                return "krakow.bus.zip", "https://gtfs.ztp.krakow.pl/GTFS_KRK_A.zip"

The script now needs to be run as ``python krakow_gtfs.py tram`` or ``python krakow_gtfs.py bus``.

The last thing we'd want to do is to save the fixed data back to GTFS - we can use the
:py:class:`~impuls.tasks.SaveGTFS` task for that. Unfortunately, it requires manually providing the
GTFS headers, so its definition can be quite long. We'll also use the ``type`` command line argument
to save the file into ``_impuls_workspace/krakow.tram.out.zip`` or ``krakow.bus.out.zip``::

    impuls.tasks.SaveGTFS(
        headers={
            "agency": ("agency_id", "agency_name", "agency_url", "agency_timezone", "agency_lang", "agency_phone"),
            "stops": ("stop_id", "stop_code", "stop_name", "stop_lat", "stop_lon"),
            "routes": ("agency_id", "route_id", "route_short_name", "route_long_name", "route_type", "route_color", "route_text_color"),
            "trips": ("route_id", "service_id", "trip_id", "trip_headsign", "direction_id"),
            "stop_times": ("trip_id", "stop_sequence", "stop_id", "arrival_time", "departure_time"),
            "calendar": ("service_id", "start_date", "end_date", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"),
            "calendar_dates": ("service_id", "date", "exception_type"),
        },
        target=options.workspace_directory / f"krakow.{args.type}.out.zip",
    ),

And that's it - you now have successfully used Impuls to fix a GTFS file.


Converting data to GTFS (PKP Intercity)
---------------------------------------

The input data doesn't have to be in the GTFS format to be loaded into Impuls.
As long as there is a way to convert your input data into the expected :doc:`/db_schema`
in a :py:class:`~impuls.Task` (single or many), this library can be used for data processing.

To demonstrate this we'll convert PKP Interity (Polish train operator) data into GTFS.
The source data comes from the `Polish MMTIS National Access Point <https://dane.gov.pl/pl/dataset/1739,krajowy-punkt-dostepowy-kpd-multimodalne-usugi-informacji-o-podrozach>`_
and unfortunately to access the original files one needs to email the agency to obtain FTP access credentials.

The source data is a single, Windows-1250-encoded CSV file embedded in a zip archive
on an ftp server. The csv contains 21 columns, but only the following fields are relevant for our exercise:

* ``DataOdjazdu`` - departure date from the first station
* ``NrPociagu`` - train number, unique within its departure date
* ``NrPociaguHandlowy`` - user-facing train number
* ``NazwaPociagu`` - train name
* ``NumerStacji`` - station ID
* ``NazwaStacji`` - station name
* ``StacjaHandlowa`` - is the station available for passengers?
* ``Przyjazd`` - arrival wall time
* ``Odjazd`` - departure wall time
* ``KategoriaHandlowa`` - train category
* ``PeronWyjazd`` - departure platform
* ``BUS`` - is departure replaced by a bus?

.. csv-table:: Example rows of single train (only relevant columns and rows are shown)
    :header-rows: 1

    DataOdjazdu,NrPociagu,NrPociaguHandlowy,NazwaPociagu,NumerStacji,NazwaStacji,StacjaHandlowa,Przyjazd,Odjazd,KategoriaHandlowa,PeronWyjazd,BUS
    2024-08-03,13104/5,13104,WITOS,38653,Warszawa Wschodnia,1,05:52:30,05:57:00,IC,I,0
    2024-08-03,13104/5,13104,WITOS,33605,Warszawa Centralna,1,06:03:00,06:12:00,IC,IV,0
    2024-08-03,13104/5,13104,WITOS,33506,Warszawa Zachodnia,1,06:16:00,06:24:00,IC,VIII,0
    2024-08-03,13104/5,13104,WITOS,33563,Warszawa Służewiec,1,06:32:00,06:33:00,IC,I,0
    2024-08-03,13104/5,13104,WITOS,33902,Piaseczno,1,06:41:00,06:43:00,IC,I,0
    2024-08-03,13104/5,13104,WITOS,48504,Warka,1,07:03:00,07:04:00,IC,II,0
    2024-08-03,13104/5,13104,WITOS,48355,Radom Główny,1,07:31:00,07:33:00,IC,II,0
    2024-08-03,13104/5,13104,WITOS,48033,Skarżysko Kościelne,1,08:02:30,08:03:30,IC,I,0
    2024-08-03,13104/5,13104,WITOS,48181,Starachowice Wschodnie,1,08:13:30,08:14:30,IC,II,0
    2024-08-03,13104/5,13104,WITOS,49205,Ostrowiec Świętokrzyski,1,08:33:00,08:49:00,IC,BUS,1
    2024-08-03,13104/5,13104,WITOS,65300,Sandomierz,1,09:44:00,09:54:00,IC,I,0
    2024-08-03,13104/5,13104,WITOS,65003,Stalowa Wola Rozwadów,1,10:22:00,10:24:00,IC,II,0
    2024-08-03,13104/5,13104,WITOS,65029,Stalowa Wola Centrum,1,10:27:00,10:28:00,IC,I,0
    2024-08-03,13104/5,13104,WITOS,65094,Nisko,1,10:35:00,10:36:00,IC,I,0
    2024-08-03,13104/5,13104,WITOS,65144,Rudnik n/Sanem,1,10:44:30,10:49:00,IC,II,0
    2024-08-03,13104/5,13104,WITOS,83246,Nowa Sarzyna,1,11:07:00,11:08:00,IC,I,0
    2024-08-03,13104/5,13104,WITOS,83220,Leżajsk,1,11:15:00,11:16:00,IC,I,0
    2024-08-03,13104/5,13104,WITOS,83105,Przeworsk,1,11:38:00,11:39:00,IC,I,0
    2024-08-03,13104/5,13104,WITOS,83261,Jarosław,1,11:48:00,11:49:00,IC,II,0
    2024-08-03,13104/5,13104,WITOS,84301,Radymno,1,11:57:30,11:58:30,IC,I,0
    2024-08-03,13104/5,13104,WITOS,84434,Przemyśl Zasanie,1,12:12:00,12:13:00,IC,I,0
    2024-08-03,13104/5,13104,WITOS,84400,Przemyśl Główny,1,12:16:00,12:21:00,IC,I,0

Station locations need to be pulled from an external source - https://github.com/MKuranowski/PLRailMap.

To start processing PKP Intercity data, we need to first get the CSV schedules from the
FTP server. Extracting files from a zip archive is provided with the
:py:class:`impuls.resource.ZippedResource` adaptor, but we still need to implement
:py:class:`impuls.Resource` to get the compressed file from FTP.

Unfortunately, the `builtin FTP client <https://docs.python.org/3/library/ftplib.html>`_ can't
be used as-is. 3 modifications need to be made:

- the IP address sent in the PASV response needs to be ignored, ftps.intercity.pl sends garbage data,
- support for the MDTM command needs to be added (to fetch file modification times),
- a way to receive files as ``Iterable[bytes]`` needs to be added, instead of the
  callback-based `FTP.retrbinary <https://docs.python.org/3/library/ftplib.html#ftplib.FTP.retrbinary>`_.

To cut a long-story short, the necessary patches look like this::

    from datetime import datetime, timezone
    from ftplib import FTP_TLS

    class PatchedFTP(FTP_TLS):
        def makepasv(self) -> tuple[str, int]:
            _, port = super().makepasv()
            return self.host, port

        def iter_binary(self, cmd: str, blocksize: int = 8192) -> Iterator[bytes]:
            # See the implementation of FTP.retrbinary. This is the same, but instead of
            # using the callback we just yield the data.
            self.voidcmd("TYPE I")
            with self.transfercmd(cmd) as conn:
                while data := conn.recv(blocksize):
                    yield data
            return self.voidresp()

        def mod_time(self, filename: str) -> datetime:
            resp = self.voidcmd(f"MDTM {filename}")
            return self.parse_ftp_mod_time(resp.partition(" ")[2])

        @staticmethod
        def parse_ftp_mod_time(x: str) -> datetime:
            if len(x) == 14:
                return datetime.strptime(x, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
            elif len(x) > 15:
                return datetime.strptime(x[:21], "%Y%m%d%H%M%S.%f").replace(tzinfo=timezone.utc)
            else:
                raise ValueError(f"invalid FTP mod_time: {x}")

With the patched FTP client, we are ready to create our own class implementing :py:class:`impuls.Resource`.
This interface contains 2 attributes and 1 method - we'll use a `dataclass <https://docs.python.org/3/library/dataclasses.html>`_
to help with those attributes::

    import impuls
    from dataclasses import dataclass
    from datetime import datetime, timezone
    from typing import Iterable

    @dataclass
    class FTPResource:
        host: str
        filename: str
        username: str
        password: str

        last_modified: datetime = impuls.resource.DATETIME_MIN_UTC
        fetch_time: datetime = impuls.resource.DATETIME_MIN_UTC

        def fetch(self, conditional: bool) -> Iterable[bytes]:
            with PatchedFTP(self.host, self.username, self.password) as ftp:
                ftp.prot_p()

                current_last_modified = ftp.mod_time(self.filename)
                if conditional and current_last_modified <= self.last_modified:
                    raise impuls.errors.InputNotModified

                self.last_modified = current_last_modified
                self.fetch_time = datetime.now(timezone.utc)
                yield from ftp.iter_binary(f"RETR {self.filename}")

Note that :py:class:`impuls.Resource` requires us to keep track of file modification time
(to support ``conditional`` requests) and document download times (mostly for legal reasons).
FTP doesn't support conditional requests, so we simply compare curent modification time with
the cached one before performing the download. I'd recon that not that many protocols support
conditional requests, but for example HTTP has the If-Modified-Since and If-None-Match headers.

With the ``FTPResource`` implemented we are ready to declare an :py:class:`impuls.App` with
the required resources::

    import argparse
    import impuls

    class PKPIntercityGTFS(impuls.App):
        def add_arguments(self, parser: argparse.ArgumentParser) -> None:
            parser.add_argument("username", help="ftps.intercity.pl username")
            parser.add_argument("password", help="ftps.intercity.pl password")

        def prepare(self, args: argparse.Namespace, options: impuls.PipelineOptions) -> impuls.Pipeline:
            return Pipeline(
                options=options,
                tasks=[],
                resources={
                    "rozklad_kpd.csv": impuls.resource.ZippedResource(
                        FTPResource("ftps.intercity.pl", "rozklad/KPD_Rozklad.zip", args.username, args.password),
                        file_name_in_zip="KPD_Rozklad.csv",
                    ),
                    "pl_rail_map.osm": HTTPResource.get("https://raw.githubusercontent.com/MKuranowski/PLRailMap/master/plrailmap.osm"),
                },
            )

    if __name__ == "__main__":
        PKPIntercityGTFS("PKPIntercityGTFS").run()

Let's now move onto the task of loading the CSV file into the database.
As the first thing we can create the :py:class:`~impuls.model.Agency` representing PKP Intercity::

    impuls.tasks.AddEntity(
        impuls.model.Agency(
            id="0",
            name="PKP Intercity",
            url="https://intercity.pl",
            timezone="Europe/Warsaw",
            lang="pl",
            phone="+48703200200",
        ),
        task_name="AddAgency",
    )

Let's now move onto processing the CSV itself, and for that, we need to prepare our own :py:class:`~impuls.Task`.
Fortunately, the CSV is sorted by the departure date, train number and stop_sequence (in that order),
so we can leverage `itertools.groupby <https://docs.python.org/3/library/itertools.html#itertools.groupby>`_
and `operator.itemgetter <https://docs.python.org/3/library/operator.html#operator.itemgetter>`_
to easily extracts trains from the input file. :py:attr:`impuls.TaskRuntime.resources` values provide
a :py:meth:`~impuls.resource.ManagedResource.csv` method to eaily parse the CSV file
(see :py:class:`impuls.resource.ManagedResource` reference for more helper methods).
Since :py:class:`routes <impuls.model.Route>`, :py:class:`stops <impuls.model.Stop>` and
:py:class:`calendars <impuls.model.Calendar>` are not explicitly provided, we'll need to create them
on the fly. To avoid duplicates, we'll need to keep track of which objects were already added.
Therefore, the task initialization and main loop can look like this::

    import impuls
    from operator import itemgetter
    from itertools import groupby

    class ImportCSV(impuls.Task):
        def __init__(self, csv_resource_name: str, agency_id: str = "0") -> None:
            super().__init__()
            self.csv_resource_name = csv_resource_name
            self.agency_id = agency_id

            self.saved_routes = set[str]()
            self.saved_stops = set[str]()
            self.saved_calendars = set[str]()

        def clear(self) -> None:
            self.saved_routes.clear()
            self.saved_stops.clear()
            self.saved_calendars.clear()

        def execute(self, r: impuls.TaskRuntime) -> None:
            self.clear()
            with r.db.transaction():
                csv_reader = r.resources[self.csv_resource_name].csv(encoding="windows-1250", delimiter=";")
                grouped_departures = groupby(
                    filter(lambda row: row["StacjaHandlowa"] == "1", csv_reader),
                    itemgetter("DataOdjazdu", "NrPociagu")
                )
                for _, train_departures in trains:
                    self.save_train(list(train_departures), r.db)

To save a train we're going to first extract and prettify user-facing attributes
(especially the :py:attr:`Trip.short_name <impuls.model.Trip.short_name>` - which we'll be the train
number and its name), ensure the parent :py:class:`~impuls.model.Route` and :py:class:`~impuls.model.Calendar`
exist. Then, the :py:class:`~impuls.Trip` and :py:class:`StopTimes <impuls.model.StopTime>` are going
to be added::

    from impuls.model import Calendar, Date, Route, Stop, StopTime, TimePoint, Trip

    class ImportCSV(impuls.Task):
        def save_train(self, rows: list[dict[str, str]], db: impuls.DBConnection) -> None:
            route_id = rows[0]["KategoriaHandlowa"]
            number = rows[0]["NrPociaguHandlowy"]
            if not number:
                number = rows[0]["NrPociagu"].partition("/")[0]
            name = rows[0]["NazwaPociagu"]
            calendar_id = rows[0]["DataOdjazdu"]
            trip_id = f'{calendar_id}_{rows[0]["NrPociagu"].replace("/", "-")}'
            headsign = rows[-1]["NazwaStacji"]

            if name and number in name:
                short_name = name.title().replace("Zka", "ZKA")
            elif name:
                short_name = f"{number} {name.title()}"
            else:
                short_name = number

            self.save_route(route_id, db)
            self.save_calendar(calendar_id, db)
            db.create(
                Trip(
                    id=trip_id,
                    route_id=route_id,
                    calendar_id=calendar_id,
                    headsign=headsign,
                    short_name=short_name,
                )
            )
            self.save_departures(rows, trip_id, db)

        def save_route(self, route_id: str, db: DBConnection) -> None:
            if route_id not in self.saved_routes:
                self.saved_routes.add(route_id)
                db.create(Route(route_id, self.agency_id, route_id, "", Route.Type.RAIL))

        def save_stop(self, stop_id: str, stop_name: str, db: DBConnection) -> None:
            if stop_id not in self.saved_stops:
                self.saved_stops.add(stop_id)
                db.create(Stop(stop_id, stop_name, 0.0, 0.0))

        def save_calendar(self, calendar_id: str, db: DBConnection) -> None:
            if calendar_id not in self.saved_calendars:
                self.saved_calendars.add(calendar_id)
                date = Date.from_ymd_str(calendar_id)
                db.create(
                    Calendar(
                        calendar_id,
                        monday=True,
                        tuesday=True,
                        wednesday=True,
                        thursday=True,
                        friday=True,
                        saturday=True,
                        sunday=True,
                        start_date=date,
                        end_date=date,
                    )
                )

Saving :py:class:`StopTimes <impuls.model.StopTime>` comes with 3 caveats: we need to ensure that
the relevant :py:class:`~impuls.model.Stop` exists, convert every wall time to
:py:class:`~impuls.model.TimePoint` (relevant for trips crossing midnight, CSV time sequence [23:55, 00:01]
needs to be saved as [23:55, 24:01]) and preserve replacement bus departures through the
:py:attr:`StopTime.platform <impuls.model.StopTime.platform>` field::

    class ImportCSV(impuls.Task):
        def save_departures(self, rows: list[dict[str, str]], trip_id: str, db: impuls.DBConnection) -> None:
            previous_departure = TimePoint(seconds=0)
            for idx, row in enumerate(rows):
                stop_id = row["NumerStacji"]
                self.save_stop(stop_id, row["NazwaStacji"], db)

                platform = row["PeronWyjazd"]
                if row["BUS"] == "1":
                    platform = "BUS"
                elif platform in ("NULL", "BUS"):
                    platform = ""

                arrival = TimePoint.from_str(row["Przyjazd"])
                while arrival < previous_departure:
                    arrival = TimePoint(seconds=(arrival + DAY).total_seconds())

                departure = TimePoint.from_str(row["Odjazd"])
                while departure < arrival:
                    departure = TimePoint(seconds=(departure + DAY).total_seconds())

                db.create(
                    StopTime(
                        trip_id=trip_id,
                        stop_id=stop_id,
                        stop_sequence=idx,
                        arrival_time=arrival,
                        departure_time=departure,
                        platform=platform,
                    )
                )
                previous_departure = departure

        def save_stop(self, stop_id: str, stop_name: str, db: DBConnection) -> None:
            if stop_id not in self.saved_stops:
                self.saved_stops.add(stop_id)
                db.create(Stop(stop_id, stop_name, 0.0, 0.0))

As mentioned earlier, the stop locations need to come from another source. For now, we put
all train stations at the `Null Island <https://en.wikipedia.org/wiki/Null_Island>`_. Preserving
information about bus replacement services allows for correctly splitting the trains into
trips assigned to :py:obj:`~impuls.model.Route.Type.BUS` and :py:obj:`~impuls.model.Route.Type.RAIL`.
This tutorial doesn't show the implementation of a task doing the splitting, but the
`full example code includes a SplitBusLegs task <https://github.com/MKuranowski/Impuls/blob/main/examples/pkpic/split_bus_legs.py>`_.

The task of importing the CSV is now completed! We can move onto loading station data.
Since the PLRailMap data is stored using the `OSM XML <https://wiki.openstreetmap.org/wiki/OSM_XML>`_
format, we'll use `osmiter <https://pypi.org/project/osmiter/>`_ to help us load it. The idea is simple -
we loop over all stations from the PLRailMap project, updating stop positions in the database as we go. We need to keep track
of stops which need don't have positions. There's also another problem: some stations have 2 different ids,
so we need to cleverly ensure that the primary one is used. The task can be implemented as following::

    import impuls
    import osmiter

    class ImportStationData(impuls.Task) -> None:
        def __init__(self, pl_rail_map_resource: str) -> None:
            super().__init__()
            self.pl_rail_map_resource = pl_rail_map_resource

        def execute(self, r: TaskRuntime) -> None:
            to_import = {
                cast(str, i[0]): cast(str, i[1])
                for i in r.db.raw_execute("SELECT stop_id, name FROM stops")
            }

            # Iterate over stations from PLRailMap
            pl_rail_map_path = r.resources[self.pl_rail_map_resource].stored_at
            for elem in osmiter.iter_from_osm(pl_rail_map_path, file_format="xml", filter_attrs=set()):
                if elem["type"] != "node" or elem["tag"].get("railway") != "station":
                    continue

                id = elem["tag"]["ref"]
                id2 = elem["tag"].get("ref:2")

                # Skip unused stations
                if id not in to_import and id2 not in to_import:
                    continue

                # Update stop data, ensuring the primary ID is used
                if id in to_import:
                    r.db.raw_execute(
                        "UPDATE stops SET name = ?, lat = ?, lon = ? WHERE stop_id = ?",
                        (elem["tag"]["name"], elem["lat"], elem["lon"], id),
                    )
                else:
                    r.db.raw_execute(
                        "INSERT INTO stops (stop_id, name, lat, lon) VALUES (?, ?, ?, ?)",
                        (id, elem["tag"]["name"], elem["lat"], elem["lon"]),
                    )

                # Remove references to the secondary ID
                if id2 in to_import:
                    r.db.raw_execute("UPDATE stop_times SET stop_id = ? WHERE stop_id = ?", (id, id2))
                    r.db.raw_execute("DELETE FROM stops WHERE stop_id = ?", (id2,))

                # Remove entries from to_import
                to_import.pop(id, None)
                to_import.pop(id2, None)

            # Warn on unused stops
            r.db.raw_execute_many("DELETE FROM stops WHERE stop_id = ?", ((k,) for k in to_import))
            for id, name in to_import.items():
                self.logger.warn("No data for station %s (%s)", id, name)

The basic conversion of PKP Intercity data is done! We can close it all of with by exporting the
schedules as GTFS, which gives the following list of tasks::

    tasks = [
        impuls.tasks.AddEntity(
            impuls.model.Agency(
                id="0",
                name="PKP Intercity",
                url="https://intercity.pl",
                timezone="Europe/Warsaw",
                lang="pl",
                phone="+48703200200",
            ),
            task_name="AddAgency",
        ),
        ImportCSV("rozklad_kpd.csv"),
        ImportStationData("pl_rail_map.osm"),
        impuls.tasks.SaveGTFS(
            headers={
                "agency": ("agency_id", "agency_name", "agency_url", "agency_timezone", "agency_lang", "agency_phone"),
                "stops": ("stop_id", "stop_name", "stop_lat", "stop_lon"),
                "routes": ("agency_id", "route_id", "route_short_name", "route_long_name", "route_type", "route_color", "route_text_color"),
                "trips": ("route_id", "service_id", "trip_id", "trip_headsign", "trip_short_name"),
                "stop_times": ("trip_id", "stop_sequence", "stop_id", "arrival_time", "departure_time", "platform"),
                "calendar": ("service_id", "start_date", "end_date", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"),
            },
            target=options.workspace_directory / f"pkpic.zip",
        )
    ]

There are still other small things which can be done to increase the quality of the data.
Some of data polishing is included in the `full example code <https://github.com/MKuranowski/Impuls/tree/main/examples/pkpic>`_.
