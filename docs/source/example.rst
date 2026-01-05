Example Usage
=============

Impuls contains multiple various bits and pieces to help process transit
data, and it it not entirely obvious how to use this library from the
:doc:`/api/index`. Following through this tutorial should give you a good idea
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
e.g. ``impuls~=2.4.1``, or any other way to force a specific major version of the library.


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
        KrakowGTFS().run()

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

With the patched FTP client, we are ready to create our own class implementing :py:class:`impuls.Resource`
(through the :py:class:`impuls.resource.ConcreteResource` base class)::

    import impuls
    from dataclasses import dataclass
    from datetime import datetime, timezone
    from typing import Iterable

    @dataclass
    class FTPResource(impuls.resource.ConcreteResource):
        def __init__(
            self,
            host: str
            filename: str,
            username: str,
            password: str,
        ) -> None:
            super().__init__()
            self.host = host
            self.filename = filename
            self.username = username
            self.password = password

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
        PKPIntercityGTFS().run()

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
                self.logger.warning("No data for station %s (%s)", id, name)

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


Combining multiple files/versions into a single dataset (Radom)
---------------------------------------------------------------

Some agencies push out a new file with each schedules update. Converting every
individual file/version to a separete GTFS dataset would violate the `GTFS specification <https://gtfs.org/schedule/reference/#dataset-publishing-general-practices>`_.
To create a high-quality all of the versions need to be loaded and merged together
to form a single coherent timetable package.

This problem seems to be especially prevalent in Poland, see datasets from
`ZTM Poznań <https://www.ztm.poznan.pl/pl/dla-deweloperow/gtfsFiles>`_,
`GZM ZTM (Katowice) <https://otwartedane.metropoliagzm.pl/dataset/rozklady-jazdy-i-lokalizacja-przystankow-gtfs-wersja-rozszerzona>`_,
`ZTM Warszawa <ftp://rozklady.ztm.waw.pl>`_ and `MZDiK Radom <https://mzdik.pl/index.php?id=145>`_.

In this section of the tutorial, we'll use the :py:mod:`impuls.multi_file` module to
help us automatically process the intermediate schedules, merge them and create a single,
high-quality, merged dataset.

We're going to process data from Radom, which gives out MDB (Microsoft Access/JET) database
exports from BusMan. Impuls comes with a task to load such files - :py:class:`~impuls.tasks.LoadBusManMDB`.
The dataset isn't complete - stop data needs to be loaded from http://rkm.mzdik.radom.pl/,
and calendar data will be created with the help of :py:mod:`impuls.tools.polish_calendar_exceptions`.

Let's start by writing the :py:class:`impuls.App` for Radom::

    import argparse
    import impuls

    class RadomGTFS(impuls.App):
        def prepare(
            self, args: argparse.Namespace, options: impuls.PipelineOptions,
        ) -> impuls.multi_file.MultiFile[impuls.Resource]:
            return impuls.multi_file.MultiFile(
                options=options,
                # intermediate_provider=  # TODO
                # intermediate_pipeline_tasks_factory=  # TODO
                # final_pipeline_tasks_factory=  # TODO
                additional_resources={},
            )

    if __name__ == "__main__":
        RadomGTFS().run()


The first thing we need is a :py:class:`~impuls.multi_file.IntermediateFeedProvider`. It's going to
provide intermediate files to process to :py:class:`~impuls.multi_file.MultiFile`. For Radom,
the implementation will scrape database files from https://mzdik.pl/index.php?id=145 with
`requests <https://pypi.org/project/requests/>`_ and `lxml <https://pypi.org/project/lxml/>`_.
The mdb databases are compressed in a zip archive, so we're going to use the :py:class:`impuls.resource.ZippedResource`
adaptor::

    import re
    from io import StringIO
    from urllib.parse import urljoin

    import requests
    from lxml import etree

    from impuls.model import Date
    from impuls.multi_file import IntermediateFeed, IntermediateFeedProvider, prune_outdated_feeds
    from impuls.resource import HTTPResource, ZippedResource

    LIST_URL = "http://mzdik.pl/index.php?id=145"

    class RadomProvider(IntermediateFeedProvider[ZippedResource]):
        def __init__(self, for_date: Date | None = None) -> None:
            self.for_date = for_date or Date.today()

        def needed(self) -> list[IntermediateFeed[ZippedResource]]:
            # Request the website
            with requests.get(LIST_URL) as r:
                r.raise_for_status()
                r.encoding = "utf-8"

            # Parse the website
            tree = etree.parse(StringIO(r.text), etree.HTMLParser())

            # Find links to schedule files and collect feeds
            feeds: list[IntermediateFeed[ZippedResource]] = []
            for anchor in tree.xpath("//a"):
                href = anchor.get("href", "")
                if not re.search(r"/upload/file/Rozklady.+\.zip", href):
                    continue

                version_match = re.search(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", href)
                if not version_match:
                    raise ValueError(f"unable to get feed_version from href {href!r}")
                version = version_match[0]

                feed = IntermediateFeed(
                    ZippedResource(HTTPResource.get(urljoin(LIST_URL, href))),
                    resource_name=f"Rozklady-{version}.mdb",
                    version=version,
                    start_date=Date.from_ymd_str(version),
                )
                feeds.append(feed)

            prune_outdated_feeds(feeds, self.for_date)
            return feeds

We can now add this provider to the main :py:class:`~impuls.multi_file.MultiFile` factory.
While we're here, we can also narrow down the resource type of that class, as we now know we're
providing :py:class:`~impuls.resource.ZippedResource`::

    def prepare(
        self, args: argparse.Namespace, options: impuls.PipelineOptions,
    ) -> impuls.multi_file.MultiFile[impuls.resource.ZippedResource]:
        return impuls.multi_file.MultiFile(
            options=options,
            intermediate_provider=RadomProvider(),
            # intermediate_pipeline_tasks_factory=  # TODO
            # final_pipeline_tasks_factory=  # TODO
            additional_resources={},
        )

The next thing we need to prepare is the :py:obj:`~impuls.multi_file.MultiFile.intermediate_pipeline_tasks_factory`.
This function needs to take the :py:class:`~impuls.multi_file.IntermediateFeed` returned by ``RadomProvider``
and create a list of tasks to import that file. Let's start by simply importing the file using
:py:class:`~impuls.tasks.LoadBusManMDB`, which requires us to create an :py:class:`~impuls.model.Agency` first::

    intermediate_pipeline_tasks_factory = lambda feed: [
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
        LoadBusManMDB(
            feed.resource_name,
            agency_id="0",
            ignore_route_id=True,
            ignore_stop_id=False,
        ),
    ]

Unfortunately, the MDB databases don't contain all necessary data for creating a full Impuls/GTFS
dataset - we're missing :py:class:`~impuls.model.Calendar` details and :py:class:`~impuls.model.Stop`
positions. As mentioned earlier, we're going to load the latter from http://rkm.mzdik.radom.pl/,
and generate the former with the help of :py:mod:`impuls.tools.polish_calendar_exceptions`.
But before that, we need to do a bit of data cleaning - removing technical/virtual stops and unknown calendars::

    intermediate_pipeline_tasks_factory = lambda feed: [
        # ...
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
            statement="DELETE FROM calendars WHERE desc NOT IN ('POWSZEDNI', 'SOBOTA', 'NIEDZIELA')",
        ),
    ]

Thank to the ``RetainKnownCalendars`` task, we know we only have 3 calendars to deal with:
workdays ("POWSZEDNI" :py:attr:`~impuls.model.Calendar.desc`), saturdays ("SOBOTA" :py:attr:`~impuls.model.Calendar.desc`)
and sunday ("NIEDZIELA" :py:attr:`~impuls.model.Calendar.desc`). The last calendar also applies
on public holidays, so we're going to need to generate appropriate :py:class:`CalendarExceptions <impuls.model.CalendarException>`.
The main logic of the :py:class:`~impuls.Task` can look like this::

    from impuls import DBConnection, Task, TaskRuntime
    from impuls.model import Date
    from impuls.resource import ManagedResource
    from impuls.tools.polish_calendar_exceptions import CalendarExceptionType, PolishRegion, load_exceptions
    from impuls.tools.temporal import BoundedDateRange

    class GenerateCalendars(Task):
        def __init__(self, start_date: Date) -> None:
            super().__init__()
            self.range = BoundedDateRange(start_date, start_date.add_days(365))

            self.weekday_id = ""
            self.saturday_id = ""
            self.sunday_id = ""

        def execute(self, r: TaskRuntime) -> None:
            self.set_calendar_ids(r.db)
            with r.db.transaction():
                self.update_calendar_entries(r.db)
                self.generate_calendar_exceptions(r.db, r.resources["calendar_exceptions.csv"])

Even though we're generating calendar data for a year, this is not going to pose a problem
when merging - :py:class:`~impuls.mutli_file.MultiFile` automatically runs the
:py:class:`~impuls.tasks.TruncateCalendars` task in the :py:obj:`pre-merge pipeline <impuls.multi_file.MultiFile.pre_merge_pipeline_tasks_factory>`.
Retriving the calendar IDs from the database is pretty simple::

    from typing import cast

    class GenerateCalendars(Task):
        # ...

        def set_calendar_ids(self, db: DBConnection) -> None:
            self.weekday_id = self.get_calendar_id("POWSZEDNI", db)
            self.saturday_id = self.get_calendar_id("SOBOTA", db)
            self.sunday_id = self.get_calendar_id("NIEDZIELA", db)

        def get_calendar_id(self, desc: str, db: DBConnection) -> str:
            result = db.raw_execute("SELECT calendar_id FROM calendars WHERE desc = ?", (desc,))
            row = result.one_must(f"Missing calendar with description {desc!r}")
            return cast(str, row[0])

Updating :py:class:`Calendars <impuls.model.Calendar>` also boils down to a couple UPDATE statements::

    class GenerateCalendars(Task):
        # ...

        def update_calendar_entries(self, db: DBConnection) -> None:
            db.raw_execute(
                "UPDATE calendars SET start_date = ?, end_date = ?",
                (str(self.range.start), str(self.range.end)),
            )
            db.raw_execute(
                "UPDATE calendars SET "
                "    monday = 1,"
                "    tuesday = 1,"
                "    wednesday = 1,"
                "    thursday = 1,"
                "    friday = 1,"
                "    saturday = 0,"
                "    sunday = 0 "
                "  WHERE calendar_id = ?",
                (self.weekday_id,),
            )
            db.raw_execute(
                "UPDATE calendars SET "
                "    monday = 0,"
                "    tuesday = 0,"
                "    wednesday = 0,"
                "    thursday = 0,"
                "    friday = 0,"
                "    saturday = 1,"
                "    sunday = 0 "
                "  WHERE calendar_id = ?",
                (self.saturday_id,),
            )
            db.raw_execute(
                "UPDATE calendars SET "
                "    monday = 0,"
                "    tuesday = 0,"
                "    wednesday = 0,"
                "    thursday = 0,"
                "    friday = 0,"
                "    saturday = 0,"
                "    sunday = 1 "
                "  WHERE calendar_id = ?",
                (self.sunday_id,),
            )

The last part is to generate :py:class:`CalendarExceptions <impuls.model.CalendarException>` for
public holidays. We'll use :py:func:`impuls.tools.polish_calendar_exceptions.load_exceptions_for`
to get all of public holidays, and then insert appropriate entries into the ``calendar_exceptions``
table::

    from impuls.tools.polish_calendar_exceptions import (
        CalendarExceptionType,
        PolishRegion,
        load_exceptions_for,
    )

    class GenerateCalendars(Task):
        # ...

        def generate_calendar_exceptions(
            self, db: DBConnection, calendar_exceptions_resource: ManagedResource,
        ) -> None:
            exceptions = load_exceptions(calendar_exceptions_resource, PolishRegion.MAZOWIECKIE)
            for date, exception in exceptions.items():
                # Ignore exceptions outside of the requested range
                if date not in self.range:
                    continue

                # Ignore anything that's not a holiday
                if CalendarExceptionType.HOLIDAY not in exception.typ:
                    continue

                date_str = str(date)
                weekday = date.weekday()

                if weekday == 6:
                    # If a holiday falls on a sunday - not an exception
                    pass

                elif weekday == 5:
                    # Holiday falls on saturday - replace
                    db.raw_execute_many(
                        "INSERT INTO calendar_exceptions (calendar_id, date, exception_type) "
                        "VALUES (?, ?, ?)",
                        ((self.sunday_id, date_str, 1), (self.saturday_id, date_str, 2)),
                    )

                else:
                    # Holiday falls on a workday - replace
                    db.raw_execute_many(
                        "INSERT INTO calendar_exceptions (calendar_id, date, exception_type) "
                        "VALUES (?, ?, ?)",
                        ((self.sunday_id, date_str, 1), (self.weekday_id, date_str, 2)),
                    )

That's it for generating calendars; we can now deal with stop data.

Impuls comes with a built-in :py:class:`~impuls.tasks.ModifyStopsFromCSV` task, too bad
that http://rkm.mzdik.radom.pl/ returns stops in the XML format. Well, we can do a little trick
and convert the XML to CSV on the fly in the :py:class:`~impuls.ConcreteResource` implementation.
To interact with the SOAP service, we're going to use the `zeep <https://pypi.org/project/zeep/>`_
package. The course of action is simply - get the stops from the ``GetGoogleStops`` endpoint of
http://rkm.mzdik.radom.pl/PublicService.asmx, convert them to CSV, and return the CSV file::

    from datetime import datetime, timezone
    from typing import Iterator

    import zeep
    from impuls.resource import FETCH_CHUNK_SIZE, ConcreteResource

    class RadomStopsResource(ConcreteResource):
        def fetch(self, conditional: bool) -> Iterator[bytes]:
            # Fetch stops from Radom's SOAP service
            self.fetch_time = datetime.now(timezone.utc)
            self.last_modified = self.fetch_time
            client = zeep.Client("http://rkm.mzdik.radom.pl/PublicService.asmx?WSDL")
            service = client.create_service(
                r"{http://PublicService/}PublicServiceSoap",
                "http://rkm.mzdik.radom.pl/PublicService.asmx",
            )
            stops = service.GetGoogleStops().findall("S")

            if len(stops) == 0:
                raise RuntimeError("no stops returned from rkm.mzdik.radom.pl")

            # Dump the stops to a csv
            buffer = BytesIO()
            text_buffer = TextIOWrapper(buffer, encoding="utf-8", newline="")
            writer = csv.writer(text_buffer)
            writer.writerow(("stop_id", "stop_name", "stop_lat", "stop_lon"))
            for stop in stops:
                writer.writerow((
                    stop.attrib["id"],
                    stop.get("n", "").strip(),
                    stop.get("y", ""),
                    stop.get("x", ""),
                ))
            text_buffer.flush()

            # Yield CSV data
            buffer.seek(0)
            while chunk := buffer.read(FETCH_CHUNK_SIZE):
                yield chunk

We can now complete the ``intermediate_pipeline_tasks_factory``::

    intermediate_pipeline_tasks_factory = lambda feed: [
        # ...
        GenerateCalendars(feed.start_date),
        ModifyStopsFromCSV("soap_stops.csv"),
    ]

And we need to add the stops and calendar exceptions resources as well::

    from impuls.tools import polish_calendar_exceptions

    additional_resources = {
        "calendar_exceptions.csv": polish_calendar_exceptions.RESOURCE,
        "soap_stops.csv": RadomStopsResource(),
    }

The last thing to do is to create the :py:obj:`~impuls.multi_file.MultiFile.final_pipeline_tasks_factory`.
There's nothing to do after the data is merged, so we can simply save the processed data to GTFS::

    final_pipeline_tasks_factory = lambda _: [
        impuls.tasks.SaveGTFS(
            headers={
                "agency": ("agency_id", "agency_name", "agency_url", "agency_timezone", "agency_lang"),
                "stops": ("stop_id", "stop_name", "stop_lat", "stop_lon"),
                "routes": ("agency_id", "route_id", "route_short_name", "route_long_name", "route_type"),
                "trips": ("route_id", "service_id", "trip_id"),
                "stop_times": ("trip_id", "stop_sequence", "stop_id", "arrival_time", "departure_time"),
                "calendar": ("service_id", "start_date", "end_date", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday", "service_desc"),
                "calendar_dates": ("service_id", "date", "exception_type"),
            },
            target=options.workspace_directory / "radom.zip",
        )
    ]

That's it! We now have succefully processed Radom data spread across multiple files into a single
GTFS.
