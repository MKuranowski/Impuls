from collections import defaultdict

from impuls.model import CalendarException, Date
from impuls.tasks import ExtendCalendars
from impuls.tools.temporal import date_range

from .template_testcase import AbstractTestTask


class TestExtendCalendars(AbstractTestTask.Template):
    def test(self) -> None:
        t = ExtendCalendars(start_date=Date(2024, 5, 1), duration_days=30)
        t.execute(self.runtime)

        self.assertEqual(
            t.extend_template,
            [
                Date(2024, 4, 29),
                Date(2024, 4, 30),
                Date(2024, 4, 24),
                Date(2024, 4, 25),
                Date(2024, 4, 26),
                Date(2024, 4, 27),
                Date(2024, 4, 28),
            ],
        )

        calendars_by_date = defaultdict[Date, set[str]](set)
        for e in self.runtime.db.retrieve_all(CalendarException):
            self.assertIs(e.exception_type, CalendarException.Type.ADDED)
            calendars_by_date[e.date].add(e.calendar_id)

        for d in date_range(Date(2024, 5, 1), Date(2024, 5, 30)):
            expected_services = {"C"} if d.weekday() >= 5 else {"D"}
            self.assertSetEqual(calendars_by_date[d], expected_services)

    def test_with_holidays(self) -> None:
        t = ExtendCalendars(
            start_date=Date(2024, 5, 1),
            duration_days=30,
            holidays={Date(2024, 4, 28), Date(2024, 5, 1), Date(2024, 5, 3)},
        )
        t.execute(self.runtime)

        self.assertEqual(
            t.extend_template,
            [
                Date(2024, 4, 29),
                Date(2024, 4, 30),
                Date(2024, 4, 24),
                Date(2024, 4, 25),
                Date(2024, 4, 26),
                Date(2024, 4, 27),
                Date(2024, 4, 21),
            ],
        )

        calendars_by_date = defaultdict[Date, set[str]](set)
        for e in self.runtime.db.retrieve_all(CalendarException):
            self.assertIs(e.exception_type, CalendarException.Type.ADDED)
            calendars_by_date[e.date].add(e.calendar_id)

        for d in date_range(Date(2024, 5, 1), Date(2024, 5, 30)):
            expected_services = {"C"} if d in t.holidays or d.weekday() >= 5 else {"D"}
            self.assertSetEqual(calendars_by_date[d], expected_services)

    def test_not_needed(self) -> None:
        t = ExtendCalendars(start_date=Date(2024, 1, 1), duration_days=30)
        with self.assertLogs(t.logger) as log_ctx:
            t.execute(self.runtime)

        self.assertEqual(len(log_ctx.records), 1)
        self.assertEqual(log_ctx.records[0].message, "Calendar extension not needed")
