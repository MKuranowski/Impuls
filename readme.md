Impuls
======

Impuls is a framework for processing static public transportation data.
The internal model used is very close to GTFS.

The core entity for processing is called a _pipeline_, which is composed of multiple
_tasks_ that do the actual processing work.

The data is stored in an sqlite3 database (in-memory by default) with a very lightweight
wrapper to map Impuls's internal model into SQL and GTFS.

Impuls has first-class support for pulling in data from external sources, using its
_resource_ mechanizm. Resources are cached before the data is processed, which saves
bandwidth if some of the input data has not changed, or even allows to stop the
processing early if none of the resources have been modified.

A module for dealing with versioned, or _multi-file_ sources is also provided. It allows
for easy and very flexible processing of schedules provided in discrete versions into
a single coherent file.

Tests
-----

Currently, tests cover around 87% of the codebase. Run with:

```terminal
$ python -m venv .venv
$ . .venv/bin/activate
$ pip install -U pip
$ pip install -Ur requirements.dev.txt
$ pytest
```

Examples
--------

The `examples` directory contains 4 example configurations, processing data
from four sources into a GTFS file. Before running the examples, run the following commands:

```terminal
$ python -m venv .venv  # Unless already run
$ . .venv/bin/activate
$ pip install -U pip    # Unless already run
$ pip install -Ur requirements.examples.txt
```

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
