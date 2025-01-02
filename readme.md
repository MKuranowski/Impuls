Impuls
======

[GitHub](https://github.com/MKuranowski/impuls) |
[Documentation](https://impuls.readthedocs.io/) |
[Issue Tracker](https://github.com/MKuranowski/impuls/issues) |
[PyPI](https://pypi.org/project/impuls/)

Impuls is a framework for processing static public transportation data.
The internal model used is very close to GTFS.

The core entity for processing is called a _pipeline_, which is composed of multiple
_tasks_ that do the actual processing work.

The data is stored in an sqlite3 database with a very lightweight
wrapper to map Impuls's internal model into SQL and GTFS.

Impuls has first-class support for pulling in data from external sources, using its
_resource_ mechanism. Resources are cached before the data is processed, which saves
bandwidth if some of the input data has not changed, or even allows to stop the
processing early if none of the resources have been modified.

A module for dealing with versioned, or _multi-file_ sources is also provided. It allows
for easy and very flexible processing of schedules provided in discrete versions into
a single coherent file.

Installation and compilation
----------------------------

Impuls is mainly written in python, however a performance-critical part of this library is written
in zig and bundled alongside the shared library. To compile and install the library,
first ensure that [zig](https://ziglang.org/learn/getting-started/) is installed, then
run the following, preferably inside of a
[virtual environment](https://docs.python.org/3/library/venv.html):

Impuls is mainly written in python, however a performance-critical part of this library is written
in zig and bundled alongside the shared library. To install the library run the following,
preferably inside of a [virtual environment](https://docs.python.org/3/library/venv.html):

```
pip install impuls
```

Pre-built binaries are available for most platforms. To build from source
[zig](https://ziglang.org/learn/getting-started/) needs to be installed.

The `LoadBusManMDB` task additionally requires [mdbtools](https://github.com/mdbtools/mdbtools)
to be installed. This package is available in most package managers.

Examples
--------

See <https://impuls.readthedocs.io/en/stable/example.html> for a tutorial and a more detailed
walkthrough over Impuls features.

The `examples` directory contains 4 example configurations, processing data
from four sources into a GTFS file. If you wish to run them, consult with the
[Development](#development) section of the readme to set up the environment correctly.

### Kraków

Kraków provides decent GTFS files on <https://gtfs.ztp.krakow.pl>.
The example pipeline removes unnecessary, confusing trip data and fixes
several user-facing strings.

Run with `python -m examples.krakow tram` or `python -m examples.krakow bus`.
The result GTFS will be created in `_workspace_krakow/krakow.tram.out.zip` or
`_workspace_krakow/krakow.bus.out.zip`, accordingly.

### PKP IC (PKP Intercity)

PKP Intercity provides their schedules in a single CSV table at <ftp://ftps.intercity.pl>.
Unfortunately, the source data is not openly available. One needs to email PKP Intercity
through the contact provided in the [Polish MMTIS NAP](https://dane.gov.pl/pl/dataset/1739,krajowy-punkt-dostepowy-kpd-multimodalne-usugi-informacji-o-podrozach)
in order to get the credentials.

The Pipeline starts by manually creating an Agency, loading the CSV data,
pulling station data from <https://github.com/MKuranowski/PLRailMap>,
adjusting some user-facing data - most importantly extracting trip legs operated by buses.

Run with `python -m examples.pkpic FTP_USERNAME FTP_PASSWORD`. The result GTFS
will be created at `_workspace_pkpic/pkpic.zip`

### Radom

MZDiK Radom provides schedules in a MDB database at <http://mzdik.pl/index.php?id=145>.
It is the first example to use the _multi-file_ pipeline support, as the source files
are published in discrete versions.

Multi-file pipelines consist of four distinct parts:
- an _intermediate provider_, which figures out the relevant input ("intermediate") feeds
- a _intermediate tasks factory_, which returns the tasks necessary to load
    an intermediate feed into the SQLite database
- a _final tasks factory_, which returns the tasks to perform after merging intermediate feeds
- any additional _resources_, required by the intermediate or final tasks

Caching is even more involved - not only the input feeds are kept across runs,
but the databases resulting from running intermediate pipelines are also preserved.
If 3 of 4 feeds requested by the intermediate provider have already been processed -
the intermediate pipeline will run only for the single new file, but the final (merging)
pipeline will be run on all of the 4 feeds.

The intermediate provider for Radom scrapes the aforementioned website to find
available databases.

Pipeline for processing intermediate feeds is a bit more complex: it involved
loading the MDB database, cleaning up the data (removing virtual stops, generating and
cleaning calendars) and pulling stop positions from <http://rkm.mzdik.radom.pl/>.

The final pipeline simply dumps the merged dataset into a GTFS.

Run with `python -m examples.radom`, the result GTFS will
be created at `_workspace_radom/radom.zip`.

### Warsaw

Warsaw is another city which requires multi-file pipelines.
ZTM Warsaw publishes distinct input files for pretty much every other day
at <ftp://rozklady.ztm.waw.pl>. The input datasets are in a completely custom
text format, requiring quite involved parsing. More details are available at
<https://www.ztm.waw.pl/pliki-do-pobrania/dane-rozkladowe/> (in Polish).

The intermediate provider picks out relevant files from the aforementioned FTP server.

Processing of intermediate feeds starts with the import of the text file into
the database. Rather uniquely, this step also prettifies stop names - as this
would be hard to do in a separate task, due to the presence of indicators
(two-digit codes uniquely identifying a stop around an intersection) in the name field.
The pipeline continues by adding version meta-data, merging railway stations into a single
stops.txt entry (ZTM separates railway departures into virtual stops) and attribute
prettifying (namely trip_headsign and stop_lat,stop_lon - not all stops have positions
in the input file). Last steps involve cleaning up unused entities from the database.

The final pipeline simply dumps the merged dataset into a GTFS, yet again.

Additional data for stop positions and edge-cases for prettifying stop names
comes from <https://github.com/MKuranowski/WarsawGTFS/blob/master/data_curated/stop_names.json>.

Run with `python -m examples.warsaw`, the result GTFS will
be created at `_workspace_warsaw/warsaw.zip`.

License
-------

Impuls is distributed under GNU GPL v3 (or any later version).

> © Copyright 2022-2025 Mikołaj Kuranowski
>
> Impuls is free software: you can redistribute it and/or modify
> it under the terms of the GNU General Public License as published by
> the Free Software Foundation; either version 3 of the License, or
> (at your option) any later version.
>
> Impuls is distributed in the hope that it will be useful,
> but WITHOUT ANY WARRANTY; without even the implied warranty of
> MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
> GNU General Public License for more details.
>
> You should have received a copy of the GNU General Public License
> along with Impuls. If not, see <http://www.gnu.org/licenses/>.

Impuls source code and pre-built binaries come with [sqlite3](https://sqlite.org/),
which [is placed in the public domain](https://www.sqlite.org/copyright.html).

Development
-----------

Impuls uses [meson-python](https://meson-python.readthedocs.io/en/latest/index.html). The
project layout is quite unorthodox, as Impuls in neither a pure-python module, nor a project
with a bog-standard C/C++ extension. Instead, the zig code is compiled into a shared library
which is bundled alongside the python module.

Zig allows super easy cross-compilation, while using a shared library allows a single wheel
to be used across multiple python versions and implementations.

Development requires [python](https://python.org/), [zig](https://ziglang.org/learn/getting-started/)
and [mdbtools](https://github.com/mdbtools/mdbtools/) (usually all 3 will be available in your
package manager repositories) to be installed. To set up the environment on Linux, run:

```terminal
$ python -m venv --upgrade-deps .venv
$ . .venv/bin/activate
$ pip install -Ur requirements.dev.txt
$ pip install --no-build-isolation -Cbuild-dir=builddir --editable .
$ ln -s ../../builddir/libextern.so impuls/extern
```

On MacOS, change the shared library file extension to `.dylib`. On Windows, change the extension
of the shared library to `.dll`.

To run python tests, simply execute `pytest`. To run zig tests, run `meson test -C builddir`.

To run the examples, install their dependencies first (`pip install -Ur requirements.examples.txt`),
then execute the example module, e.g. `python -m examples.krakow`.

meson-python will automatically recompile the zig library whenever an editable impuls install is
imported; set the `MESONPY_EDITABLE_VERBOSE` environment variable to `1` to see meson logs for build
details.

By default, the extern zig library will be built in debug mode. To change that, run
`meson configure --buildtype=debugoptimized builddir` (buildtype can also be set to `debug` or
`release`). To recompile the library, run `meson compile -C builddir`.

Unfortunately, meson-python requires all python and zig source files in meson.build. Python
files need to be listed for packaging to work, while zig source files need to be listed for
the build backend to properly detect whether libextern needs to be recompiled.

### Building wheels

Zig has been chosen for its excellent cross-compilation support. Thanks to this, building
all wheels for a release does not require tools like [cibuildwheel](https://github.com/pypa/cibuildwheel),
virtual machines, or even any containers. As long as Zig is installed, all wheels can be
build on that machine.

Before building wheels, install a few extra dependencies in the virtual environment:
`pip install -U build wheel`.

To build the wheels, simply run `python build_wheels.py`.

See `python build_wheels.py --help` for all available options. To debug failed builds, run
`python build_wheels.py --verbose --jobs 1 FAILED_CONFIG_NAME`.

See [CONFIGURATION in build_wheels.py](/build_wheels.py#L32) for available configurations.

To build the source distribution, run `python -m build -so dist`.
