from dataclasses import dataclass, field
from enum import IntEnum
from typing import ClassVar, Optional

from .impuls_base import ImpulsBase, TypeMetadata, impuls_base
from .utility_types import Date, Maybe, TimePoint

__all__ = [
    "Agency",
    "Stop",
    "Route",
    "Calendar",
    "CalendarException",
    "Trip",
    "StopTime",
    "FeedInfo",
    "Attribution",
]

# Metadata used in the classes comes in 2 forms - Type-based and Field-based
# Type-based metadata is stored in the class-variable called `_metadata`
# Field-based metadata is stored in the dataclass field.
#
# See the _TypeMetadata and _FieldMetadata types in the impuls_base module.


@impuls_base
@dataclass(unsafe_hash=True)
class Agency(ImpulsBase):
    _metadata: ClassVar[TypeMetadata] = {"table_name": "agencies"}

    id: str = field(compare=True, metadata={"primary_key": True})
    name: str = field(compare=False)
    url: str = field(compare=False, repr=False)
    timezone: str = field(compare=False, repr=False)
    lang: str = field(default="", compare=False, repr=False)
    phone: str = field(default="", compare=False, repr=False)
    fare_url: str = field(default="", compare=False, repr=False)


@impuls_base
@dataclass(unsafe_hash=True)
class Stop(ImpulsBase):
    class LocationType(IntEnum):
        STOP = 0
        STATION = 1
        EXIT = 2

    id: str = field(compare=True, metadata={"primary_key": True})
    name: str = field(compare=False)
    lat: float = field(compare=False, repr=False)
    lon: float = field(compare=False, repr=False)
    code: str = field(default="", compare=False)

    zone_id: str = field(
        default="",
        compare=False,
        repr=False,
        metadata={"gtfs_no_entity_prefix": True, "index": True},
    )

    location_type: Optional[LocationType] = field(
        default=None, compare=False, repr=False, metadata={"gtfs_no_entity_prefix": True}
    )

    parent_station: str = field(
        default="", compare=False, repr=False, metadata={"gtfs_no_entity_prefix": True}
    )

    platform_code: str = field(
        default="", compare=False, repr=False, metadata={"gtfs_no_entity_prefix": True}
    )

    pkpplk_code: str = field(
        default="", compare=False, repr=False, metadata={"gtfs_column_name": "stop_pkpplk"}
    )

    ibnr_code: str = field(
        default="", compare=False, repr=False, metadata={"gtfs_column_name": "stop_IBNR"}
    )


@impuls_base
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

    id: str = field(compare=True, metadata={"primary_key": True})

    agency_id: str = field(
        compare=False,
        repr=False,
        metadata={"foreign_key": "agencies(agency_id)"},
    )

    short_name: str = field(compare=False)
    long_name: str = field(compare=False)
    type: Type = field(compare=False)
    color: str = field(default="", compare=False, repr=False)
    text_color: str = field(default="", compare=False, repr=False)
    sort_order: Optional[int] = field(default=None, compare=False, repr=False)


@impuls_base
@dataclass(unsafe_hash=True)
class Calendar(ImpulsBase):
    _metadata: ClassVar[TypeMetadata] = {"gtfs_table_name": "calendar"}

    id: str = field(compare=True, metadata={"primary_key": True, "gtfs_column_name": "service_id"})
    monday: bool = field(compare=False, repr=False, metadata={"gtfs_no_entity_prefix": True})
    tuesday: bool = field(compare=False, repr=False, metadata={"gtfs_no_entity_prefix": True})
    wednesday: bool = field(compare=False, repr=False, metadata={"gtfs_no_entity_prefix": True})
    thursday: bool = field(compare=False, repr=False, metadata={"gtfs_no_entity_prefix": True})
    friday: bool = field(compare=False, repr=False, metadata={"gtfs_no_entity_prefix": True})
    start_date: Date = field(compare=False, repr=False, metadata={"gtfs_no_entity_prefix": True})
    end_date: Date = field(compare=False, repr=False, metadata={"gtfs_no_entity_prefix": True})


@impuls_base
@dataclass(unsafe_hash=True)
class CalendarException(ImpulsBase):
    _metadata: ClassVar[TypeMetadata] = {"gtfs_table_name": "calendar_dates"}

    class Type(IntEnum):
        ADDED = 1
        REMOVED = 2

    calendar_id: str = field(
        compare=True,
        metadata={
            "primary_key": True,
            "foreign_key": "calendars(calendar_id)",
            "gtfs_column_name": "service_id",
        },
    )

    date: Date = field(
        compare=False, metadata={"primary_key": True, "gtfs_no_entity_prefix": True}
    )

    exception_type: Type = field(
        compare=False, repr=False, metadata={"gtfs_no_entity_prefix": True}
    )


@impuls_base
@dataclass(unsafe_hash=True)
class Trip(ImpulsBase):
    class Direction(IntEnum):
        OUTBOUND = 0
        INBOUND = 1

    id: str = field(compare=True, metadata={"primary_key": True})
    route_id: str = field(compare=False, metadata={"foreign_key": "routes(route_id)"})
    calendar_id: str = field(compare=False, metadata={"foreign_key": "calendars(calendar_id)"})
    headsign: str = field(default="", compare=False)
    short_name: str = field(default="", compare=False, repr=False)
    direction: Optional[Direction] = field(
        default=None, compare=False, repr=False, metadata={"gtfs_column_name": "direction_id"}
    )

    # block_id: str = field(default="", compare=False, repr=False)
    # shape_id: str = field(default="", compare=False, repr=False)

    wheelchair_accessible: Maybe = field(
        default=Maybe.UNKNOWN, compare=False, repr=False, metadata={"gtfs_no_entity_prefix": True}
    )

    bikes_allowed: Maybe = field(
        default=Maybe.UNKNOWN, compare=False, repr=False, metadata={"gtfs_no_entity_prefix": True}
    )

    exceptional: Optional[bool] = field(
        default=None, compare=False, repr=False, metadata={"gtfs_no_entity_prefix": True}
    )


@impuls_base
@dataclass(unsafe_hash=True)
class StopTime(ImpulsBase):
    class PassengerExchange(IntEnum):
        SCHEDULED_STOP = 0
        NONE = 1
        MUST_PHONE = 2
        ON_REQUEST = 3

    trip_id: str = field(
        compare=True, metadata={"primary_key": True, "foreign_key": "trips(trip_id)"}
    )

    stop_id: str = field(compare=False, metadata={"foreign_key": "stops(stop_id)"})

    stop_sequence: int = field(
        compare=True, repr=False, metadata={"primary_key": True, "gtfs_no_entity_prefix": True}
    )

    arrival_time: TimePoint = field(
        compare=False, repr=False, metadata={"gtfs_no_entity_prefix": True}
    )

    departure_time: TimePoint = field(
        compare=False, repr=False, metadata={"gtfs_no_entity_prefix": True}
    )

    pickup_type: PassengerExchange = field(
        default=PassengerExchange.SCHEDULED_STOP,
        compare=False,
        repr=False,
        metadata={"gtfs_no_entity_prefix": True},
    )

    drop_off_type: PassengerExchange = field(
        default=PassengerExchange.SCHEDULED_STOP,
        compare=False,
        repr=False,
        metadata={"gtfs_no_entity_prefix": True},
    )

    # shape_dist_traveled: Optional[float] = field(
    #     default=None, compare=False, repr=False, metadata={"gtfs_no_entity_prefix": True}
    # )

    # original_stop_id: str = field(
    #     default="", compare=False, repr=False, metadata={"gtfs_no_entity_prefix": True}
    # )


@impuls_base
@dataclass(unsafe_hash=True)
class FeedInfo(ImpulsBase):
    _metadata: ClassVar[TypeMetadata] = {"table_name": "feed_info"}

    id: str = field(
        default="0",
        init=False,
        compare=False,
        repr=False,
        metadata={"primary_key": True},
    )

    publisher_name: str = field(
        compare=False, metadata={"gtfs_column_name": "feed_publisher_name"}
    )

    publisher_url: str = field(
        compare=False, repr=False, metadata={"gtfs_column_name": "feed_publisher_url"}
    )

    lang: str = field(compare=False, repr=False, metadata={"gtfs_column_name": "feed_lang"})

    version: str = field(default="", compare=True, metadata={"gtfs_column_name": "feed_version"})

    contact_email: str = field(
        default="", compare=False, repr=False, metadata={"gtfs_column_name": "feed_contact_email"}
    )

    contact_url: str = field(
        default="", compare=False, repr=False, metadata={"gtfs_column_name": "feed_contact_url"}
    )


@impuls_base
@dataclass(unsafe_hash=True)
class Attribution(ImpulsBase):
    id: str = field(compare=True, metadata={"primary_key": True, "implicit_generation": True})

    organization_name: str = field(compare=False, metadata={"gtfs_no_entity_prefix": True})

    is_producer: bool = field(
        default=False, compare=False, repr=False, metadata={"gtfs_no_entity_prefix": True}
    )

    is_operator: bool = field(
        default=False, compare=False, repr=False, metadata={"gtfs_no_entity_prefix": True}
    )

    is_authority: bool = field(
        default=False, compare=False, repr=False, metadata={"gtfs_no_entity_prefix": True}
    )

    is_data_source: bool = field(
        default=False, compare=False, repr=False, metadata={"gtfs_no_entity_prefix": True}
    )

    agency_id: str = field(
        default="", compare=False, repr=False, metadata={"foreign_key": "agencies(agency_id)"}
    )

    route_id: str = field(
        default="", compare=False, repr=False, metadata={"foreign_key": "routes(route_id)"}
    )

    trip_id: str = field(
        default="", compare=False, repr=False, metadata={"foreign_key": "trips(trip_id)"}
    )

    url: str = field(default="", compare=False, repr=False)
    email: str = field(default="", compare=False, repr=False)
    phone: str = field(default="", compare=False, repr=False)
