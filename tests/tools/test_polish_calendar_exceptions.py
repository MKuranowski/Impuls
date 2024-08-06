import unittest
from datetime import datetime
from pathlib import Path

from impuls.model import Date
from impuls.resource import ManagedResource
from impuls.tools.polish_calendar_exceptions import (
    CalendarExceptionType,
    PolishRegion,
    load_exceptions,
)

FIXTURE_RESOURCE = ManagedResource(
    stored_at=Path(__file__).with_name("fixtures") / "polish_calendar_exceptions.csv",
    last_modified=datetime.fromisoformat("2024-08-01T08:00:00+00:00"),
    fetch_time=datetime.fromisoformat("2024-08-01T08:00:00+00:00"),
)


class TestPolishCalendarExceptions(unittest.TestCase):
    def test_country_wide(self) -> None:
        exceptions = load_exceptions(FIXTURE_RESOURCE, PolishRegion.MAZOWIECKIE)

        # Check 2022-01-01
        self.assertIn(Date(2022, 1, 1), exceptions)
        self.assertSetEqual(exceptions[Date(2022, 1, 1)].typ, {CalendarExceptionType.HOLIDAY})
        self.assertIs(exceptions[Date(2022, 1, 1)].summer_holiday, False)
        self.assertEqual(exceptions[Date(2022, 1, 1)].holiday_name, "nowy_rok")

        # Check 2022-04-10
        self.assertIn(Date(2022, 4, 10), exceptions)
        self.assertSetEqual(
            exceptions[Date(2022, 4, 10)].typ, {CalendarExceptionType.COMMERCIAL_SUNDAY}
        )
        self.assertIs(exceptions[Date(2022, 4, 10)].summer_holiday, False)
        self.assertEqual(exceptions[Date(2022, 4, 10)].holiday_name, "")

        # Check 2022-06-26
        self.assertIn(Date(2022, 6, 26), exceptions)
        self.assertSetEqual(
            exceptions[Date(2022, 6, 26)].typ,
            {
                CalendarExceptionType.NO_SCHOOL,
                CalendarExceptionType.COMMERCIAL_SUNDAY,
            },
        )
        self.assertIs(exceptions[Date(2022, 6, 26)].summer_holiday, True)
        self.assertEqual(exceptions[Date(2022, 6, 26)].holiday_name, "")

        # Check 2022-12-25
        self.assertIn(Date(2022, 12, 25), exceptions)
        self.assertSetEqual(exceptions[Date(2022, 12, 25)].typ, {CalendarExceptionType.HOLIDAY})
        self.assertIs(exceptions[Date(2022, 12, 25)].summer_holiday, False)
        self.assertEqual(exceptions[Date(2022, 12, 25)].holiday_name, "boze_narodzenie_1")

        # Check non-exceptions
        self.assertNotIn(Date(2022, 3, 8), exceptions)
        self.assertNotIn(Date(2022, 10, 17), exceptions)

    def test_regional(self) -> None:
        exceptions_ma = load_exceptions(FIXTURE_RESOURCE, PolishRegion.MAZOWIECKIE)
        exceptions_wm = load_exceptions(FIXTURE_RESOURCE, PolishRegion.WARMINSKO_MAZURSKIE)

        # 2022-01-24 is only an exception in Warmińsko-Mazurskie
        self.assertNotIn(Date(2022, 1, 24), exceptions_ma)
        self.assertIn(Date(2022, 1, 24), exceptions_wm)

        # 2022-01-30 is an exception in both Mazowieckie and Warmińsko-Mazurskie,
        # but with different types.
        self.assertIn(Date(2022, 1, 30), exceptions_ma)
        self.assertIn(Date(2022, 1, 30), exceptions_wm)
        self.assertSetEqual(
            exceptions_ma[Date(2022, 1, 30)].typ,
            {CalendarExceptionType.COMMERCIAL_SUNDAY},
        )
        self.assertSetEqual(
            exceptions_wm[Date(2022, 1, 30)].typ,
            {
                CalendarExceptionType.COMMERCIAL_SUNDAY,
                CalendarExceptionType.NO_SCHOOL,
            },
        )
