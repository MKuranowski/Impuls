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
--------------------

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
