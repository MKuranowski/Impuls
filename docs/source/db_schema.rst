Database Schema
===============

The following is a entity-relationship diagram of the databases inside Impuls.
This model is heavily based on `GTFS <https://gtfs.org/schedule/>`_.

:class:`impuls.DBConnection` has convenience helpers to convert between the raw data
coming from SQL and :mod:`impuls.model` dataclasses.

.. figure:: /_static/database_er.*
    :scale: 15 %
    :alt: Impuls database ER diagram

    Impuls database ER diagram (click to zoom).

