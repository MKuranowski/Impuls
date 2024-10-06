import logging

from impuls.model import Calendar, CalendarException, Date
from impuls.tasks.truncate_calendars import NoServicesLeft, TruncateCalendars
from impuls.tools.temporal import EmptyDateRange, date_range

from .template_testcase import AbstractTestTask


class TestTruncateCalendars(AbstractTestTask.Template):
    db_name = None

    def setUp(self) -> None:
        super().setUp()

        # Fixture calendars for the following dates:
        #       May 2020
        # Mo Tu We Th Fr Sa Su
        #              1  2  3
        #  4  5  6  7  8  9 10
        # 18 19 20 21 22 23 24
        #
        # Mon-Thu: Calendar-based,
        # Fri: Calendar-based
        # Sat: Exception-based
        # Sun: Exception-based
        #
        # Exceptions: 1st is "Sun", not "Fri"
        self.start = Date(2020, 5, 1)
        self.end = Date(2020, 5, 24)
        with self.runtime.db.transaction():
            self.runtime.db.create_many(
                Calendar,
                [
                    Calendar(
                        "Mon-Thu",
                        monday=True,
                        tuesday=True,
                        wednesday=True,
                        thursday=True,
                        friday=False,
                        saturday=False,
                        sunday=False,
                        start_date=self.start,
                        end_date=self.end,
                    ),
                    Calendar(
                        "Fri",
                        monday=False,
                        tuesday=False,
                        wednesday=False,
                        thursday=False,
                        friday=True,
                        saturday=False,
                        sunday=False,
                        start_date=self.start,
                        end_date=self.end,
                    ),
                    Calendar(
                        "Sat",
                        monday=False,
                        tuesday=False,
                        wednesday=False,
                        thursday=False,
                        friday=False,
                        saturday=False,
                        sunday=False,
                        start_date=Date.SIGNALS_EXCEPTIONS,
                        end_date=Date.SIGNALS_EXCEPTIONS,
                    ),
                    Calendar(
                        "Sun",
                        monday=False,
                        tuesday=False,
                        wednesday=False,
                        thursday=False,
                        friday=False,
                        saturday=False,
                        sunday=False,
                        start_date=Date.SIGNALS_EXCEPTIONS,
                        end_date=Date.SIGNALS_EXCEPTIONS,
                    ),
                ],
            )
            self.runtime.db.create_many(
                CalendarException,
                [
                    CalendarException("Fri", Date(2020, 5, 1), CalendarException.Type.REMOVED),
                    CalendarException("Sat", Date(2020, 5, 2), CalendarException.Type.ADDED),
                    CalendarException("Sat", Date(2020, 5, 9), CalendarException.Type.ADDED),
                    CalendarException("Sat", Date(2020, 5, 23), CalendarException.Type.ADDED),
                    CalendarException("Sun", Date(2020, 5, 1), CalendarException.Type.ADDED),
                    CalendarException("Sun", Date(2020, 5, 3), CalendarException.Type.ADDED),
                    CalendarException("Sun", Date(2020, 5, 10), CalendarException.Type.ADDED),
                    CalendarException("Sun", Date(2020, 5, 24), CalendarException.Type.ADDED),
                ],
            )

    def test_truncates(self) -> None:
        task = TruncateCalendars(date_range(Date(2020, 5, 1), Date(2020, 5, 10)))
        task.execute(self.runtime)

        calendars = list(self.runtime.db.raw_execute("SELECT * FROM calendars"))
        calendars.sort()
        self.assertListEqual(
            calendars,
            [
                ("Fri", 0, 0, 0, 0, 0, 0, 0, "1111-11-11", "1111-11-11", "", None),
                ("Mon-Thu", 0, 0, 0, 0, 0, 0, 0, "1111-11-11", "1111-11-11", "", None),
                ("Sat", 0, 0, 0, 0, 0, 0, 0, "1111-11-11", "1111-11-11", "", None),
                ("Sun", 0, 0, 0, 0, 0, 0, 0, "1111-11-11", "1111-11-11", "", None),
            ],
        )

        exceptions = list(self.runtime.db.raw_execute("SELECT * FROM calendar_exceptions"))
        exceptions.sort()
        self.assertListEqual(
            exceptions,
            [
                ("Fri", "2020-05-08", 1),
                ("Mon-Thu", "2020-05-04", 1),
                ("Mon-Thu", "2020-05-05", 1),
                ("Mon-Thu", "2020-05-06", 1),
                ("Mon-Thu", "2020-05-07", 1),
                ("Sat", "2020-05-02", 1),
                ("Sat", "2020-05-09", 1),
                ("Sun", "2020-05-01", 1),
                ("Sun", "2020-05-03", 1),
                ("Sun", "2020-05-10", 1),
            ],
        )

    def test_removes_calendars(self) -> None:
        task = TruncateCalendars(date_range(Date(2020, 5, 1), Date(2020, 5, 3)))
        task.execute(self.runtime)

        calendars = list(self.runtime.db.raw_execute("SELECT * FROM calendars"))
        calendars.sort()
        self.assertListEqual(
            calendars,
            [
                ("Sat", 0, 0, 0, 0, 0, 0, 0, "1111-11-11", "1111-11-11", "", None),
                ("Sun", 0, 0, 0, 0, 0, 0, 0, "1111-11-11", "1111-11-11", "", None),
            ],
        )

        exceptions = list(self.runtime.db.raw_execute("SELECT * FROM calendar_exceptions"))
        exceptions.sort()
        self.assertListEqual(
            exceptions,
            [
                ("Sat", "2020-05-02", 1),
                ("Sun", "2020-05-01", 1),
                ("Sun", "2020-05-03", 1),
            ],
        )

    def test_unbound_range(self) -> None:
        task = TruncateCalendars(date_range(Date(2020, 5, 18), None))
        task.execute(self.runtime)

        calendars = list(self.runtime.db.raw_execute("SELECT * FROM calendars"))
        calendars.sort()
        self.assertListEqual(
            calendars,
            [
                ("Fri", 0, 0, 0, 0, 0, 0, 0, "1111-11-11", "1111-11-11", "", None),
                ("Mon-Thu", 0, 0, 0, 0, 0, 0, 0, "1111-11-11", "1111-11-11", "", None),
                ("Sat", 0, 0, 0, 0, 0, 0, 0, "1111-11-11", "1111-11-11", "", None),
                ("Sun", 0, 0, 0, 0, 0, 0, 0, "1111-11-11", "1111-11-11", "", None),
            ],
        )

        exceptions = list(self.runtime.db.raw_execute("SELECT * FROM calendar_exceptions"))
        exceptions.sort()
        self.assertListEqual(
            exceptions,
            [
                ("Fri", "2020-05-22", 1),
                ("Mon-Thu", "2020-05-18", 1),
                ("Mon-Thu", "2020-05-19", 1),
                ("Mon-Thu", "2020-05-20", 1),
                ("Mon-Thu", "2020-05-21", 1),
                ("Sat", "2020-05-23", 1),
                ("Sun", "2020-05-24", 1),
            ],
        )

    def test_raises_no_services_left(self) -> None:
        task = TruncateCalendars(EmptyDateRange())
        with self.assertRaises(NoServicesLeft):
            task.execute(self.runtime)

    def test_logs_no_services_left(self) -> None:
        task = TruncateCalendars(EmptyDateRange(), fail_on_empty=False)
        with self.assertLogs(task.logger, logging.WARN) as logs:
            task.execute(self.runtime)

        self.assertEqual(len(logs.output), 1)
        self.assertEqual(
            logs.output[0],
            (
                "WARNING:Task.TruncateCalendars:No services left after "
                "calendar truncation to EmptyDateRange()"
            ),
        )

        self.assertListEqual(
            list(self.runtime.db.raw_execute("SELECT * FROM calendars")),
            [],
        )
        self.assertListEqual(
            list(self.runtime.db.raw_execute("SELECT * FROM calendar_exceptions")),
            [],
        )
