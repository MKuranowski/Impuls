from typing import Type as TypeOf

from .agency import Agency
from .attribution import Attribution
from .calendar import Calendar
from .calendar_exception import CalendarException
from .feed_info import FeedInfo
from .frequency import Frequency
from .meta.entity import Entity, EntityT
from .meta.utility_types import Date, TimePoint
from .route import Route
from .shape_point import ShapePoint
from .stop import Stop
from .stop_time import StopTime
from .trip import Trip

__all__ = [
    "Agency",
    "ALL_MODEL_ENTITIES",
    "Attribution",
    "Calendar",
    "CalendarException",
    "Date",
    "FeedInfo",
    "Frequency",
    "Entity",
    "EntityT",
    "Route",
    "ShapePoint",
    "Stop",
    "StopTime",
    "TimePoint",
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
    ShapePoint,
    Trip,
    StopTime,
    Frequency,
]
