from operator import attrgetter
from pathlib import Path

from impuls import LocalResource
from impuls.model import (
    Agency,
    Attribution,
    Calendar,
    CalendarException,
    Date,
    ExtraTableRow,
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
        "wkd-extra-fields.zip": LocalResource(FIXTURES / "wkd-extra-fields.zip"),
        "wkd-extra-files.zip": LocalResource(FIXTURES / "wkd-extra-files.zip"),
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
        t = LoadGTFS("wkd-missing-routes.zip")
        with self.assertRaises(RuntimeError), self.assertLogs("impuls.extern", "ERROR") as logs:
            t.execute(self.runtime)
        self.assertIn("ERROR:impuls.extern:Missing required table routes.txt", logs.output)

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

    def test_extra_fields_false(self) -> None:
        t = LoadGTFS("wkd-extra-fields.zip", extra_fields=False)
        t.execute(self.runtime)

        agencies = sorted(
            self.runtime.db.retrieve_all(Agency),
            key=attrgetter("id"),
        )
        self.assertEqual(len(agencies), 1)
        agency = agencies[0]
        self.assertEqual(agency.id, "0")
        self.assertIsNone(agency.extra_fields_json)

        routes = sorted(
            self.runtime.db.retrieve_all(Route),
            key=attrgetter("id"),
        )
        self.assertEqual(len(routes), 3)
        route = routes[0]
        self.assertEqual(route.id, "A1")
        self.assertIsNone(route.extra_fields_json)
        route = routes[1]
        self.assertEqual(route.id, "ZA1")
        self.assertIsNone(route.extra_fields_json)
        route = routes[2]
        self.assertEqual(route.id, "ZA12")
        self.assertIsNone(route.extra_fields_json)

    def test_extra_fields_true(self) -> None:
        t = LoadGTFS("wkd-extra-fields.zip", extra_fields=True)
        t.execute(self.runtime)

        agencies = sorted(
            self.runtime.db.retrieve_all(Agency),
            key=attrgetter("id"),
        )
        self.assertEqual(len(agencies), 1)
        agency = agencies[0]
        self.assertEqual(agency.id, "0")
        self.assertDictEqual(agency.get_extra_fields(), {"agency_email": "wkd@example.com"})

        routes = sorted(
            self.runtime.db.retrieve_all(Route),
            key=attrgetter("id"),
        )
        self.assertEqual(len(routes), 3)
        route = routes[0]
        self.assertEqual(route.id, "A1")
        self.assertDictEqual(route.get_extra_fields(), {"route_is_temporary": "0"})
        route = routes[1]
        self.assertEqual(route.id, "ZA1")
        self.assertDictEqual(route.get_extra_fields(), {"route_is_temporary": "1"})
        route = routes[2]
        self.assertEqual(route.id, "ZA12")
        self.assertDictEqual(route.get_extra_fields(), {"route_is_temporary": "1"})

    def test_extra_files_empty(self) -> None:
        t = LoadGTFS("wkd-extra-files.zip")
        t.execute(self.runtime)
        self.assertEqual(self.runtime.db.count(ExtraTableRow), 0)

    def test_extra_files(self) -> None:
        t = LoadGTFS("wkd-extra-files.zip", extra_files=["counties.txt", "municipalities.txt"])
        t.execute(self.runtime)

        rows = list(self.runtime.db.retrieve_all(ExtraTableRow))
        rows.sort(key=lambda r: (r.table_name, r.row_sort_order))

        self.assertEqual(len(rows), 10)

        row = rows[0]
        self.assertEqual(row.table_name, "counties.txt")
        self.assertEqual(row.row_sort_order, 0)
        self.assertDictEqual(
            row.get_fields(),
            {"county_id": "0", "county_name": "m. st. Warszawa"},
        )

        row = rows[1]
        self.assertEqual(row.table_name, "counties.txt")
        self.assertEqual(row.row_sort_order, 1)
        self.assertDictEqual(
            row.get_fields(),
            {"county_id": "1", "county_name": "pruszkowski"},
        )

        row = rows[2]
        self.assertEqual(row.table_name, "counties.txt")
        self.assertEqual(row.row_sort_order, 2)
        self.assertDictEqual(
            row.get_fields(),
            {"county_id": "2", "county_name": "grodziski"},
        )

        row = rows[3]
        self.assertEqual(row.table_name, "municipalities.txt")
        self.assertEqual(row.row_sort_order, 0)
        self.assertDictEqual(
            row.get_fields(),
            {"municipality_id": "0", "municipality_name": "Warszawa"},
        )

        row = rows[4]
        self.assertEqual(row.table_name, "municipalities.txt")
        self.assertEqual(row.row_sort_order, 1)
        self.assertDictEqual(
            row.get_fields(),
            {"municipality_id": "1", "municipality_name": "Michałowice"},
        )

        row = rows[5]
        self.assertEqual(row.table_name, "municipalities.txt")
        self.assertEqual(row.row_sort_order, 2)
        self.assertDictEqual(
            row.get_fields(),
            {"municipality_id": "2", "municipality_name": "Pruszków"},
        )

        row = rows[6]
        self.assertEqual(row.table_name, "municipalities.txt")
        self.assertEqual(row.row_sort_order, 3)
        self.assertDictEqual(
            row.get_fields(),
            {"municipality_id": "3", "municipality_name": "Brwinów"},
        )

        row = rows[7]
        self.assertEqual(row.table_name, "municipalities.txt")
        self.assertEqual(row.row_sort_order, 4)
        self.assertDictEqual(
            row.get_fields(),
            {"municipality_id": "4", "municipality_name": "Podkowa Leśna"},
        )

        row = rows[8]
        self.assertEqual(row.table_name, "municipalities.txt")
        self.assertEqual(row.row_sort_order, 5)
        self.assertDictEqual(
            row.get_fields(),
            {"municipality_id": "5", "municipality_name": "Milanówek"},
        )

        row = rows[9]
        self.assertEqual(row.table_name, "municipalities.txt")
        self.assertEqual(row.row_sort_order, 6)
        self.assertDictEqual(
            row.get_fields(),
            {"municipality_id": "6", "municipality_name": "Grodzisk Mazowiecki"},
        )
