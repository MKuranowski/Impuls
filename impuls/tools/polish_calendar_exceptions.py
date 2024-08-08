# © Copyright 2022-2024 Mikołaj Kuranowski
# SPDX-License-Identifier: GPL-3.0-or-later

from enum import Enum
from typing import NamedTuple

from ..model.meta.utility_types import Date
from ..resource import HTTPResource, ManagedResource

RESOURCE = HTTPResource.get(
    "https://docs.google.com/spreadsheets/d/1kSCBQyIE8bz2NgqpzyS75I7ndnlp4dhD3TmEY2jO7K0"
    "/export?format=csv"
)
"""Default resource with CSV with Polish calendar exceptions from
https://docs.google.com/spreadsheets/d/1kSCBQyIE8bz2NgqpzyS75I7ndnlp4dhD3TmEY2jO7K0.
Required by :py:func:`~impuls.tools.polish_calendar_exceptions.load_exceptions`.
"""


class PolishRegion(Enum):
    """Identifies a specific voivodeship in Poland"""

    # cSpell: disable
    DOLNOSLASKIE = "02"
    KUJAWSKO_POMORSKIE = "04"
    LUBELSKIE = "06"
    LUBUSKIE = "08"
    LODZKIE = "10"
    MALOPOLSKIE = "12"
    MAZOWIECKIE = "14"
    OPOLSKIE = "16"
    PODKARPACKIE = "18"
    PODLASKIE = "20"
    POMORSKIE = "22"
    SLASKIE = "24"
    SWIETOKRZYSKIE = "26"
    WARMINSKO_MAZURSKIE = "28"
    WIELKOPOLSKIE = "30"
    ZACHODNIOPOMORSKIE = "32"
    # cSpell: enable


class CalendarExceptionType(Enum):
    """Identifies the type ("severity") of calendar exception"""

    HOLIDAY = "holiday"
    NO_SCHOOL = "no_school"
    COMMERCIAL_SUNDAY = "commercial_sunday"


class CalendarException(NamedTuple):
    """Describes a single calendar exception"""

    typ: frozenset[CalendarExceptionType]
    summer_holiday: bool = False
    holiday_name: str = ""


def load_exceptions(
    resource: ManagedResource,
    region: PolishRegion,
) -> dict[Date, CalendarException]:
    """Loads all known calendar exceptions for a specific voivodeship from the downloaded
    :py:obj:`~impuls.tools.polish_calendar_exceptions.RESOURCE`.
    """
    exceptions: dict[Date, CalendarException] = {}

    for row in resource.csv():
        date = Date.from_ymd_str(row["date"])

        # Check if the exception applies in requested region
        if row["regions"] and region.value not in row["regions"].split("."):
            continue

        # Save the exception
        exceptions[date] = CalendarException(
            frozenset(CalendarExceptionType(i) for i in row["exception"].split(".")),
            summer_holiday=row["summer_holidays"] == "1",
            holiday_name=row["holiday_name"],
        )

    return exceptions
