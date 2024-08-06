from typing import cast

from impuls import DBConnection, Task, TaskRuntime
from impuls.model import Date
from impuls.resource import ManagedResource
from impuls.tools.polish_calendar_exceptions import (
    CalendarExceptionType,
    PolishRegion,
    load_exceptions,
)
from impuls.tools.temporal import BoundedDateRange


class GenerateCalendars(Task):
    def __init__(self, start_date: Date) -> None:
        super().__init__()
        self.range = BoundedDateRange(start_date, start_date.add_days(365))

        self.weekday_id = ""
        self.saturday_id = ""
        self.sunday_id = ""

    def execute(self, r: TaskRuntime) -> None:
        self.reset_ids(r.db)
        self.update_calendar_entries(r.db)
        self.generate_calendar_exceptions(r.db, r.resources["calendar_exceptions.csv"])

    def reset_ids(self, db: DBConnection) -> None:
        self.weekday_id = self.get_calendar_id("POWSZEDNI", db)
        self.saturday_id = self.get_calendar_id("SOBOTA", db)
        self.sunday_id = self.get_calendar_id("NIEDZIELA", db)

    def update_calendar_entries(self, db: DBConnection) -> None:
        db.raw_execute(
            "UPDATE calendars SET start_date = ?, end_date = ?",
            (str(self.range.start), str(self.range.end)),
        )
        db.raw_execute(
            "UPDATE calendars SET "
            "    monday = 1,"
            "    tuesday = 1,"
            "    wednesday = 1,"
            "    thursday = 1,"
            "    friday = 1,"
            "    saturday = 0,"
            "    sunday = 0 "
            "  WHERE calendar_id = ?",
            (self.weekday_id,),
        )
        db.raw_execute(
            "UPDATE calendars SET "
            "    monday = 0,"
            "    tuesday = 0,"
            "    wednesday = 0,"
            "    thursday = 0,"
            "    friday = 0,"
            "    saturday = 1,"
            "    sunday = 0 "
            "  WHERE calendar_id = ?",
            (self.saturday_id,),
        )
        db.raw_execute(
            "UPDATE calendars SET "
            "    monday = 0,"
            "    tuesday = 0,"
            "    wednesday = 0,"
            "    thursday = 0,"
            "    friday = 0,"
            "    saturday = 0,"
            "    sunday = 1 "
            "  WHERE calendar_id = ?",
            (self.sunday_id,),
        )

    def generate_calendar_exceptions(
        self,
        db: DBConnection,
        calendar_exceptions_resource: ManagedResource,
    ) -> None:
        exceptions = load_exceptions(calendar_exceptions_resource, PolishRegion.MAZOWIECKIE)
        for date, exception in exceptions.items():
            # Ignore exceptions outside of the requested range
            if date not in self.range:
                continue

            # Ignore anything that's not a holiday
            if CalendarExceptionType.HOLIDAY not in exception.typ:
                continue

            date_str = str(date)
            weekday = date.weekday()

            if weekday == 6:
                # If a holiday falls on a sunday - not an exception
                pass

            elif weekday == 5:
                # Holiday falls on saturday - replace
                db.raw_execute_many(
                    "INSERT INTO calendar_exceptions (calendar_id, date, exception_type) "
                    "VALUES (?, ?, ?)",
                    ((self.sunday_id, date_str, 1), (self.saturday_id, date_str, 2)),
                )

            else:
                db.raw_execute_many(
                    "INSERT INTO calendar_exceptions (calendar_id, date, exception_type) "
                    "VALUES (?, ?, ?)",
                    ((self.sunday_id, date_str, 1), (self.weekday_id, date_str, 2)),
                )

    def get_calendar_id(self, desc: str, db: DBConnection) -> str:
        result = db.raw_execute("SELECT calendar_id FROM calendars WHERE desc = ?", (desc,))
        row = result.one_must(f"Missing calendar with description {desc!r}")
        return cast(str, row[0])
