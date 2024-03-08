from pathlib import Path

from impuls import LocalResource
from impuls.model import (
    Agency,
    Attribution,
    Calendar,
    CalendarException,
    Date,
    FareAttribute,
    Route,
    ShapePoint,
    Stop,
    StopTime,
    Trip,
)
from impuls.tasks import LoadGTFS

from .template_testcase import AbstractTestTask

FIXTURES = Path(__file__).with_name("fixtures")


class TestLoadGTFS(AbstractTestTask.Template):
    db_name = None
    resources = {
        "wkd.zip": LocalResource(FIXTURES / "wkd.zip"),
        "wkd-attribution-without-id.zip": LocalResource(
            FIXTURES / "wkd-attribution-without-id.zip"
        ),
        "wkd-calendar-dates-only.zip": LocalResource(FIXTURES / "wkd-calendar-dates-only.zip"),
        "wkd-missing-routes.zip": LocalResource(FIXTURES / "wkd-missing-routes.zip"),
        "wkd-no-agency-id.zip": LocalResource(FIXTURES / "wkd-no-agency-id.zip"),
    }

    def test(self) -> None:
        t = LoadGTFS("wkd.zip")
        t.execute(self.runtime)

        self.assertEqual(self.runtime.db.count(Agency), 1)
        self.assertEqual(self.runtime.db.count(Route), 3)
        self.assertEqual(self.runtime.db.count(Stop), 28)
        self.assertEqual(self.runtime.db.count(Calendar), 2)
        self.assertEqual(self.runtime.db.count(CalendarException), 14)
        self.assertEqual(self.runtime.db.count(FareAttribute), 3)
        self.assertEqual(self.runtime.db.count(ShapePoint), 1128)
        self.assertEqual(self.runtime.db.count(Trip), 372)
        self.assertEqual(self.runtime.db.count(StopTime), 6276)

    def test_missing_required_table(self) -> None:
        self.skipTest("extern.load_gtfs can't raise KeyError")
        t = LoadGTFS("wkd-missing-routes.zip")
        with self.assertRaises(KeyError):
            t.execute(self.runtime)

    def test_missing_agency_id(self) -> None:
        t = LoadGTFS("wkd-no-agency-id.zip")
        t.execute(self.runtime)

        self.assertEqual(self.runtime.db.count(Agency), 1)
        self.assertEqual(self.runtime.db.count(Route), 3)

        agency = self.runtime.db.retrieve_must(Agency, "(missing)")
        self.assertEqual(agency.id, "(missing)")
        self.assertEqual(agency.name, "Warszawska Kolej Dojazdowa")

        for route in self.runtime.db.retrieve_all(Route):
            self.assertEqual(route.agency_id, "(missing)")

    def test_missing_calendar_txt(self) -> None:
        t = LoadGTFS("wkd-calendar-dates-only.zip")
        t.execute(self.runtime)

        self.assertEqual(self.runtime.db.count(Calendar), 2)
        self.assertEqual(self.runtime.db.count(CalendarException), 31)

        for calendar in self.runtime.db.retrieve_all(Calendar):
            self.assertEqual(calendar.compressed_weekdays, 0)
            self.assertEqual(calendar.start_date, Date.SIGNALS_EXCEPTIONS)
            self.assertEqual(calendar.end_date, Date.SIGNALS_EXCEPTIONS)

        for calendar_exception in self.runtime.db.retrieve_all(CalendarException):
            self.assertEqual(calendar_exception.exception_type, CalendarException.Type.ADDED)

    def test_missing_attribution_id(self) -> None:
        t = LoadGTFS("wkd-attribution-without-id.zip")
        t.execute(self.runtime)

        self.assertEqual(self.runtime.db.count(Attribution), 1)

        # The generated id is 2, as the first record is on the 2nd line
        self.runtime.db.retrieve_must(Attribution, "2")
