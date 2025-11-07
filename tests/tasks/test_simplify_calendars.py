import dataclasses

from impuls.model import Calendar, CalendarException, Date
from impuls.tasks import SimplifyCalendars

from .template_testcase import AbstractTestTask

FIXTURE_CALENDARS = [
    Calendar(
        id="C1",
        start_date=Date.SIGNALS_EXCEPTIONS,
        end_date=Date.SIGNALS_EXCEPTIONS,
    ),
    Calendar(
        id="C2",
        saturday=True,
        sunday=True,
        start_date=Date(2025, 11, 1),
        end_date=Date(2025, 11, 7),
    ),
    Calendar(
        id="D1",
        monday=True,
        tuesday=True,
        wednesday=True,
        thursday=True,
        start_date=Date(2025, 11, 1),
        end_date=Date(2025, 11, 7),
    ),
    Calendar(
        id="D2",
        start_date=Date.SIGNALS_EXCEPTIONS,
        end_date=Date.SIGNALS_EXCEPTIONS,
    ),
]

FIXTURE_EXCEPTIONS = [
    CalendarException("C1", Date(2025, 11, 1), CalendarException.Type.ADDED),
    CalendarException("C1", Date(2025, 11, 2), CalendarException.Type.ADDED),
    CalendarException("C1", Date(2025, 11, 3), CalendarException.Type.ADDED),
    CalendarException("C2", Date(2025, 11, 3), CalendarException.Type.ADDED),
    CalendarException("D1", Date(2025, 11, 3), CalendarException.Type.REMOVED),
    CalendarException("D1", Date(2025, 11, 7), CalendarException.Type.ADDED),
    CalendarException("D2", Date(2025, 11, 4), CalendarException.Type.ADDED),
    CalendarException("D2", Date(2025, 11, 5), CalendarException.Type.ADDED),
    CalendarException("D2", Date(2025, 11, 6), CalendarException.Type.ADDED),
    CalendarException("D2", Date(2025, 11, 7), CalendarException.Type.ADDED),
]


class TestSimplifyCalendars(AbstractTestTask.Template):
    db_name = None

    def setUp(self) -> None:
        super().setUp()
        with self.runtime.db.transaction():
            self.runtime.db.create_many(Calendar, FIXTURE_CALENDARS)
            self.runtime.db.create_many(CalendarException, FIXTURE_EXCEPTIONS)

    def test_reuse_ids(self) -> None:
        task = SimplifyCalendars(generate_new_ids=False)
        task.execute(self.runtime)

        calendars = {c.id: c for c in self.runtime.db.retrieve_all(Calendar)}
        self.assertDictEqual(calendars, {"C1": FIXTURE_CALENDARS[0], "D1": FIXTURE_CALENDARS[2]})

        calendar_exceptions = self.runtime.db.retrieve_all(CalendarException).all()
        self.assertListEqual(
            calendar_exceptions,
            [i for i in FIXTURE_EXCEPTIONS if i.calendar_id in calendars],
        )

    def test_new_ids(self) -> None:
        task = SimplifyCalendars(generate_new_ids=True, id_prefix="test:")
        task.execute(self.runtime)

        calendars = {c.id: c for c in self.runtime.db.retrieve_all(Calendar)}
        self.assertDictEqual(
            calendars,
            {
                "test:0": dataclasses.replace(FIXTURE_CALENDARS[0], id="test:0"),
                "test:1": dataclasses.replace(FIXTURE_CALENDARS[2], id="test:1"),
            },
        )

        calendar_exceptions = self.runtime.db.retrieve_all(CalendarException).all()
        self.assertListEqual(
            calendar_exceptions,
            [
                dataclasses.replace(i, calendar_id="test:0")
                for i in FIXTURE_EXCEPTIONS
                if i.calendar_id == "C1"
            ]
            + [
                dataclasses.replace(i, calendar_id="test:1")
                for i in FIXTURE_EXCEPTIONS
                if i.calendar_id == "D1"
            ],
        )
