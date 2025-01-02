# © Copyright 2022-2025 Mikołaj Kuranowski
# SPDX-License-Identifier: GPL-3.0-or-later

from typing import Type as TypeOf

from .agency import Agency
from .attribution import Attribution
from .calendar import Calendar
from .calendar_exception import CalendarException
from .extra_table_row import ExtraTableRow
from .fare_attribute import FareAttribute
from .fare_rule import FareRule
from .feed_info import FeedInfo
from .frequency import Frequency
from .meta.entity import Entity, EntityT
from .meta.extra_fields_mixin import ExtraFieldsMixin
from .meta.utility_types import Date, TimePoint
from .route import Route
from .shape_point import ShapePoint
from .stop import Stop
from .stop_time import StopTime
from .transfer import Transfer
from .translation import Translation
from .trip import Trip

__all__ = [
    "Agency",
    "ALL_MODEL_ENTITIES",
    "Attribution",
    "Calendar",
    "CalendarException",
    "Date",
    "ExtraTableRow",
    "FareAttribute",
    "FareRule",
    "FeedInfo",
    "Frequency",
    "Entity",
    "EntityT",
    "ExtraFieldsMixin",
    "Route",
    "ShapePoint",
    "Stop",
    "StopTime",
    "TimePoint",
    "Transfer",
    "Translation",
    "Trip",
]

# NOTE: Ordering of classes represents loading order -
#       e.g. Trip is before StopTime, as StopTime references Trip.id
ALL_MODEL_ENTITIES: list[TypeOf[Entity]] = [
    Agency,
    Attribution,
    Calendar,
    CalendarException,
    FeedInfo,
    Route,
    Stop,
    FareAttribute,
    FareRule,
    ShapePoint,
    Trip,
    StopTime,
    Frequency,
    Transfer,
    Translation,
    ExtraTableRow,
]
"""List of all :py:class:`Entity` classes which belong to the Impuls data model.
The list is ordered to allow marshalling without KEY violations, e.g. :py:class:`Trip` is
before :py:class:`StopTime` as the latter references :py:attr:`Trip.id`.
"""
