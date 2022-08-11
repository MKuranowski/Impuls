from typing import Type as _Type
from .entities import (
    Agency,
    Attribution,
    Calendar,
    CalendarException,
    FeedInfo,
    Route,
    Stop,
    StopTime,
    Trip,
)
from .impuls_base import ImpulsBase
from .utility_types import Date, Maybe, TimePoint

ALL_MODEL_ENTITIES: list[_Type[ImpulsBase]] = [
    Agency,
    Attribution,
    Calendar,
    CalendarException,
    FeedInfo,
    Route,
    Stop,
    Trip,
    StopTime,
]
