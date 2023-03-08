import csv
import io
from enum import Enum
from typing import NamedTuple

import requests

from ..model.meta.utility_types import Date


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


def _do_load_exceptions_csv() -> io.StringIO:
    """Actually performs the request to Google Sheet
    and returns a text file-like object with raw CSV data"""
    with requests.get(
        "https://docs.google.com/spreadsheets/d/1kSCBQyIE8bz2NgqpzyS75I7ndnlp4dhD3TmEY2jO7K0"
        "/export?format=csv"
    ) as r:
        r.raise_for_status()
        r.encoding = "utf-8"
        return io.StringIO(r.text)


def load_exceptions_for(region: PolishRegion) -> dict[Date, CalendarException]:
    """Loads all known calendar exceptions for a specific voivodeship
    from an external Google Sheet."""
    exceptions: dict[Date, CalendarException] = {}
    exceptions_csv_stream = _do_load_exceptions_csv()

    for row in csv.DictReader(exceptions_csv_stream):
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
