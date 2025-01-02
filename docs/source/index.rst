Impuls
======

`GitHub <https://github.com/MKuranowski/impuls>`_ |
`Documentation <https://impuls.readthedocs.io/>`_ |
`Issue Tracker <https://github.com/MKuranowski/impuls/issues>`_ |
`PyPI <https://pypi.org/project/impuls/>`_

Impuls is a framework for processing static public transportation data.
The internal model used is very close to GTFS.

The core entity for processing is called a *pipeline*, which is composed of multiple
*tasks* that do the actual processing work.

The data is stored in an sqlite3 database with a very lightweight
wrapper to map Impuls's internal model into SQL and GTFS.

Impuls has first-class support for pulling in data from external sources, using its
*resource* mechanism. Resources are cached before the data is processed, which saves
bandwidth if some of the input data has not changed, or even allows to stop the
processing early if none of the resources have been modified.

.. figure:: /_static/pipeline.*
    :scale: 40 %
    :alt: pipeline visualization

    Diagram of basic data-processing components of Impuls

A module for dealing with versioned, or *multi-file* sources is also provided. It allows
for easy and very flexible processing of schedules provided in discrete versions into
a single coherent file.

.. figure:: /_static/multi_file.*
    :scale: 40 %
    :alt: multi-file pipeline visualization

    Diagram of multi-file/"versioned" data-processing in Impuls


Installation
------------

Impuls is published on `PyPI <https://pypi.org/project/impuls/>`_. Install by running
``pip install impuls`` inside of a `venv <https://docs.python.org/3/library/venv.html>`_.

Impuls comes with a pre-compiled shared library for performance-critical tasks. Wheels
are Python implementation and version agnostic, and are available for most common platforms
(glibc Linux, musl Linux, MacOS, Windows; both x86_64 and ARM64). Installing on other platforms
necessitate compilation form scratch, and for that `zig <https://ziglang.org/learn/getting-started/>`_
needs to be installed.


Table of Contents
-----------------

.. toctree::
    :maxdepth: 2

    db_schema
    example
    migration
    api/index
    license
