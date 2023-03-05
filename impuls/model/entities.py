from dataclasses import dataclass, field
from enum import IntEnum
from typing import Mapping, Optional, Sequence
from typing import Type as TypeOf
from typing import cast, final

from typing_extensions import LiteralString

from ..tools.types import Self, SQLNativeType
from .impuls_base import ImpulsBase
from .utility_types import Date, Maybe, TimePoint


@final
@dataclass(unsafe_hash=True)
class Route(ImpulsBase):
    class Type(IntEnum):
        TRAM = 0
        METRO = 1
        RAIL = 2
        BUS = 3
        FERRY = 4
        CABLE_TRAM = 5
        GONDOLA = 6
        FUNICULAR = 7
        TROLLEYBUS = 11
        MONORAIL = 12

    id: str = field(compare=True)
    agency_id: str = field(compare=False, repr=False)
    short_name: str = field(compare=False)
    long_name: str = field(compare=False)
    type: Type = field(compare=False)
    color: str = field(default="", compare=False, repr=False)
    text_color: str = field(default="", compare=False, repr=False)
    sort_order: Optional[int] = field(default=None, compare=False, repr=False)


@final
@dataclass(unsafe_hash=True)
class Calendar(ImpulsBase):
    id: str = field(compare=True)
    monday: bool = field(compare=False, repr=False)
    tuesday: bool = field(compare=False, repr=False)
    wednesday: bool = field(compare=False, repr=False)
    thursday: bool = field(compare=False, repr=False)
    friday: bool = field(compare=False, repr=False)
    saturday: bool = field(compare=False, repr=False)
    sunday: bool = field(compare=False, repr=False)
    start_date: Date = field(compare=False, repr=False)
    end_date: Date = field(compare=False, repr=False)
    desc: str = field(default="", compare=False, repr=False)


@final
@dataclass(unsafe_hash=True)
class Trip(ImpulsBase):
    class Direction(IntEnum):
        OUTBOUND = 0
        INBOUND = 1

    id: str = field(compare=True)
    route_id: str = field(compare=False)
    calendar_id: str = field(compare=False)
    headsign: str = field(default="", compare=False)
    short_name: str = field(default="", compare=False, repr=False)
    direction: Optional[Direction] = field(default=None, compare=False, repr=False)
    # block_id: str = field(default="", compare=False, repr=False)
    # shape_id: str = field(default="", compare=False, repr=False)
    wheelchair_accessible: Maybe = field(default=Maybe.UNKNOWN, compare=False, repr=False)
    bikes_allowed: Maybe = field(default=Maybe.UNKNOWN, compare=False, repr=False)
    exceptional: Optional[bool] = field(default=None, compare=False, repr=False)


@final
@dataclass(unsafe_hash=True)
class StopTime(ImpulsBase):
    class PassengerExchange(IntEnum):
        SCHEDULED_STOP = 0
        NONE = 1
        MUST_PHONE = 2
        ON_REQUEST = 3

    trip_id: str = field(compare=True)
    stop_id: str = field(compare=False)
    stop_sequence: int = field(compare=True, repr=False)
    arrival_time: TimePoint = field(compare=False, repr=False)
    departure_time: TimePoint = field(compare=False, repr=False)

    pickup_type: PassengerExchange = field(
        default=PassengerExchange.SCHEDULED_STOP, compare=False, repr=False
    )

    drop_off_type: PassengerExchange = field(
        default=PassengerExchange.SCHEDULED_STOP, compare=False, repr=False
    )

    stop_headsign: str = field(default="", compare=False, repr=False)

    # shape_dist_traveled: Optional[float] = field(default=None, compare=False, repr=False)
    # original_stop_id: str = field(default="", compare=False, repr=False)


@final
@dataclass(unsafe_hash=True)
class FeedInfo(ImpulsBase):
    publisher_name: str = field(compare=False)
    publisher_url: str = field(compare=False, repr=False)
    lang: str = field(compare=False, repr=False)
    version: str = field(default="", compare=True)
    contact_email: str = field(default="", compare=False, repr=False)
    contact_url: str = field(default="", compare=False, repr=False)
    id: str = field(default="0", compare=False, repr=False)


@final
@dataclass(unsafe_hash=True)
class Attribution(ImpulsBase):
    id: str = field(compare=True)
    organization_name: str = field(compare=False)
    is_producer: bool = field(default=False, compare=False, repr=False)
    is_operator: bool = field(default=False, compare=False, repr=False)
    is_authority: bool = field(default=False, compare=False, repr=False)
    is_data_source: bool = field(default=False, compare=False, repr=False)
    url: str = field(default="", compare=False, repr=False)
    email: str = field(default="", compare=False, repr=False)
    phone: str = field(default="", compare=False, repr=False)
