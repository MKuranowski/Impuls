Impuls
======

Impuls is a framework for processing static public transportation data.
The internal model used is very close to GTFS.

The core entity for processing is called a _pipeline_, which is composed of multiple
_tasks_ that do the actual processing work.
The first and last tasks are called _import_ and _export_ tasks and shouldn't do
any processing per se - rather they load and dump the internal database.

The data is stored in an sqlite3 database (in-memory by default) with a very lightweight
wrapper to map Impuls's internal model into SQL and GTFS.

In the future some form of sub-pipelines and/or meta operations on pipelines
should be written to ease processing of data which comes in discrete versions into
a single output file.

Feeds to migrate
----------------

| Done | Name                | Format       | Multi-file? |
|------|---------------------|--------------|-------------|
| [ ]  | Kielce              | GTFS         | no          |
| [ ]  | Toruń               | GTFS         | no          |
| [ ]  | Koleje Dolnośląskie | GTFS         | no          |
| [ ]  | Kraków              | GTFS         | no          |
| [ ]  | Gdańsk+Gdynia       | GTFS         | no          |
| [ ]  | Koleje Mazowieckie  | Sitkol XML   | no          |
| [ ]  | Rzeszów             | TransXChange | no          |
| [ ]  | Włocławek           | Custom, CSV  | no          |
| [ ]  | PKP Intercity       | Custom, CSV  | no          |
| [ ]  | Ełk                 | Scraped      | no          |
| [ ]  | Giżycko             | Scraped, KP  | no          |
| [ ]  | Łomża               | Scraped, KP  | no          |
| [ ]  | Świnoujście         | Scraped, KP  | no          |
| [ ]  | Rybnik              | Scraped      | no          |
| [ ]  | PolRegio            | Scraped      | no          |
| [ ]  | Lublin              | DBF          | yes         |
| [ ]  | Bydgoszcz           | BusMan MDB   | yes         |
| [ ]  | Radom               | BusMan MDB   | yes         |
| [ ]  | Warsaw              | Custom, TXT  | yes         |
