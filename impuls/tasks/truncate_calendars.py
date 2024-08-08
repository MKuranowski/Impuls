# © Copyright 2022-2024 Mikołaj Kuranowski
# SPDX-License-Identifier: GPL-3.0-or-later

from ..db import DBConnection
from ..errors import DataError
from ..model import Calendar, CalendarException, Date
from ..task import Task, TaskRuntime
from ..tools.temporal import DateRange


class NoServicesLeft(DataError):
    """NoServicesLeft is raised by :py:class:`TruncateCalendars` when all calendars are removed."""

    def __init__(self, target: DateRange) -> None:
        self.target = target
        super().__init__(f"No services left after calendar truncation to {self.target}")


class TruncateCalendars(Task):
    """TruncateCalendars removes any services beyond the provided range.

    For simplicity, all :py:class:`Calendars <impuls.model.Calendar>` are converted to
    exception-based (all active dates represented by :py:class:`~impuls.model.CalendarException`).
    """

    target: DateRange
    fail_on_empty: bool

    _to_drop: set[str]
    _to_update: dict[str, set[Date]]

    def __init__(self, target: DateRange, fail_on_empty: bool = True) -> None:
        super().__init__()
        self.target = target
        self.fail_on_empty = fail_on_empty

        self._to_drop = set()
        self._to_update = {}

    def clear_state(self) -> None:
        self._to_drop.clear()
        self._to_update.clear()

    def execute(self, r: TaskRuntime) -> None:
        self.clear_state()
        self.compute_changes(r.db)
        self.check_if_empty()
        with r.db.transaction():
            self.apply_changes(r.db)

    def compute_changes(self, db: DBConnection) -> None:
        self.logger.info("Computing changes to perform")
        for calendar in db.retrieve_all(Calendar):
            truncated_dates = self.compute_truncated_days_of(calendar, db)
            if not truncated_dates:
                self._to_drop.add(calendar.id)
            else:
                self._to_update[calendar.id] = truncated_dates

    def compute_truncated_days_of(self, calendar: Calendar, db: DBConnection) -> set[Date]:
        current_dates = CalendarException.reflect_in_active_dates(
            calendar.compute_active_dates(),
            db.typed_out_execute(
                "SELECT * FROM calendar_exceptions WHERE calendar_id = ?",
                CalendarException,
                (calendar.id,),
            ),
        )
        return {date for date in current_dates if date in self.target}

    def check_if_empty(self) -> None:
        if not self._to_update:
            if self.fail_on_empty:
                raise NoServicesLeft(self.target)
            else:
                self.logger.warning(f"No services left after calendar truncation to {self.target}")

    def apply_changes(self, db: DBConnection) -> None:
        self.drop_calendars(db)
        self.make_all_calendars_use_exceptions(db)
        self.set_exceptions_on_calendars(db)

    def drop_calendars(self, db: DBConnection) -> None:
        self.logger.info("Dropping %d calendar(s)", len(self._to_drop))
        db.raw_execute_many(
            "DELETE FROM calendars WHERE calendar_id = ?",
            ((calendar_id,) for calendar_id in self._to_drop),
        )

    def make_all_calendars_use_exceptions(self, db: DBConnection) -> None:
        self.logger.info("Updating dates of %d calendar(s)", len(self._to_update))
        exceptions_date_str = Date.SIGNALS_EXCEPTIONS.strftime("%Y-%m-%d")
        db.raw_execute(
            (
                "UPDATE calendars SET monday=0, tuesday=0, wednesday=0, thursday=0, friday=0, "
                "saturday=0, sunday=0, start_date=?, end_date=?"
            ),
            (exceptions_date_str, exceptions_date_str),
        )

    def set_exceptions_on_calendars(self, db: DBConnection) -> None:
        db.raw_execute("DELETE FROM calendar_exceptions")
        db.raw_execute_many(
            "INSERT INTO calendar_exceptions (calendar_id, date, exception_type) VALUES (?, ?, ?)",
            (
                (calendar_id, date.strftime("%Y-%m-%d"), 1)
                for calendar_id, dates in self._to_update.items()
                for date in dates
            ),
        )
