# © Copyright 2025 Mikołaj Kuranowski
# SPDX-License-Identifier: GPL-3.0-or-later

from collections import defaultdict
from collections.abc import Container, Sequence
from datetime import date
from typing import Final

from ..db import DBConnection
from ..model import Calendar, CalendarException, Date
from ..resource import ManagedResource
from ..task import Task, TaskRuntime
from ..tools import polish_calendar_exceptions
from ..tools.temporal import BoundedDateRange


class ExtendCalendars(Task):
    """ExtendCalendars ensures the GTFS has calendar data for :py:attr:`duration_days`
    starting from :py:attr:`start_date`.

    This is done by copying the services from the latest covered Monday over every
    Monday without services, and so on for every weekday. :py:attr:`fallback_weekdays`
    allow alternative weekdays to be used as the source for copying over a specific weekday.

    Holidays are never used as the source for copying schedules, and Sunday schedules
    are used when a target day is a holiday. A set of holidays dates must be explicitly provided
    in the constructor.

    As a side effect, all :py:class:`Calendars <impuls.model.Calendar>` are converted to
    exception-based (all active dates represented by :py:class:`~impuls.model.CalendarException`).
    """

    WEEKDAY_NAMES: Final[Sequence[str]] = ("mon", "tue", "wed", "thu", "fri", "sat", "sun")

    DEFAULT_FALLBACK_WEEKDAYS: Final[Sequence[Sequence[int]]] = [
        [1, 2, 3, 4],  # mon: fallback to tue, wed, thu or fri
        [2, 3, 0, 4],  # tue: fallback to wed, thu, mon or fri
        [1, 3, 0, 4],  # wed: fallback to tue, thu, mon or fri
        [1, 2, 0, 4],  # thu: fallback to wed, tue, mon or fri
        [3, 2, 1, 0],  # fri: fallback to thu, wed, tue or mon
        [6],  # sat: fallback to sun
        [5],  # sun: fallback to sat
    ]

    duration_days: int
    """Expected period of validity of the GTFS starting from the
    :py:attr:`computed start date <start_date>`. Defaults to 30.
    """

    start_date: Date | None
    """The start day of the expected coverage. If ``None``, defaults to the
    earliest date covered. Mostly useful to be overridden by ``Date.today()``.
    """

    fallback_weekdays: Sequence[Sequence[int]]
    """Lookup table for alternative weekdays which can be used to extend coverage.

    For example, the algorithm copies the latest Monday calendars over every Monday
    which should, but doesn't have any coverage. However, if the dataset contains
    absolutely no usable Mondays with service, the first weekday with service in
    ``fallback_weekdays[0]`` will be used as the sources when extending calendars
    over uncovered Mondays.

    Defaults to the following fallbacks (see :py:attr:`DEFAULT_FALLBACK_WEEKDAYS`):

    * Monday: fallback to Tuesday, Wednesday, Thursday or Friday;
    * Tuesday: fallback to Wednesday, Thursday, Monday or Friday;
    * Wednesday: fallback to Tuesday, Thursday, Monday or Friday;
    * Thursday: fallback to Wednesday, Tuesday, Monday or Friday;
    * Friday: fallback to Thursday, Wednesday, Tuesday or Monday;
    * Saturday: fallback to Sunday;
    * Sunday: fallback to Saturday.
    """

    holidays: Container[date]
    """Set of days which are never used as the source for copying services,
    and where Sunday schedules will be used when extending coverage.

    A good idea is to provide an object from the
    `holidays module <https://pypi.org/project/holidays/>`_.

    When omitted in the constructor, defaults to a new, empty set. This may be
    exploited by sub-classes, which lazily load holidays from a resource, like
    :py:class:`ExtendCalendarsFromPolishExceptions`.

    Note that extra logic may be implemented by overriding :py:meth:`is_holiday`,
    which defaults to a simple ``day in self.holidays``.
    """

    def __init__(
        self,
        duration_days: int = 30,
        start_date: Date | None = None,
        fallback_weekdays: Sequence[Sequence[int]] = DEFAULT_FALLBACK_WEEKDAYS,
        holidays: Container[date] | None = None,
    ) -> None:
        super().__init__()
        self.duration_days = duration_days
        self.start_date = start_date
        self.fallback_weekdays = fallback_weekdays
        self.holidays = holidays or set()

        if len(self.fallback_weekdays) != 7:
            raise ValueError(
                "fallback_weekdays must have 7 elements (for each weekday), "
                f"got {len(self.fallback_weekdays)}"
            )

        self.calendar_by_day = defaultdict[Date, set[str]](set)
        self.current_coverage: BoundedDateRange = _fake_date_range()
        self.expected_coverage: BoundedDateRange = _fake_date_range()
        self.extend_template = list[Date | None]()

    def execute(self, r: TaskRuntime) -> None:
        self.load_calendar(r.db)
        self.compute_coverages()
        if self.is_extension_necessary():
            self.find_extend_template()
            self.extend_calendars()
            self.update_calendars(r.db)
        else:
            self.logger.info("Calendar extension not needed")

    def find_extend_template(self) -> None:
        self.find_base_extend_template()
        self.fill_extend_template_with_fallback_days()

    def find_base_extend_template(self) -> None:
        self.extend_template: list[Date | None] = [None] * 7
        for day in self.calendar_by_day:
            if not self.is_holiday(day):
                weekday = day.weekday()
                self.extend_template[weekday] = _max_date(day, self.extend_template[weekday])

    def fill_extend_template_with_fallback_days(self) -> None:
        assert len(self.extend_template) == 7
        for weekday in range(7):
            if self.extend_template[weekday] is None:
                fallback = self.find_fallback_day_for_weekday(weekday)
                self.extend_template[weekday] = fallback
                if fallback is None:
                    self.logger.error(
                        "No template schedules for extending over %s",
                        self.WEEKDAY_NAMES[weekday],
                    )
                else:
                    self.logger.warning(
                        "Using %s (%s) schedules for extending over %s",
                        fallback,
                        fallback.weekday(),
                        weekday,
                    )

    def find_fallback_day_for_weekday(self, weekday: int) -> Date | None:
        for fallback_weekday in self.fallback_weekdays[weekday]:
            candidate = self.extend_template[fallback_weekday]
            if candidate is not None:
                return candidate
        return None

    def load_calendar(self, db: DBConnection) -> None:
        self.calendar_by_day.clear()

        for calendar in db.retrieve_all(Calendar):
            for day in calendar.compute_active_dates():
                self.calendar_by_day[day].add(calendar.id)

        for exception in db.retrieve_all(CalendarException):
            if exception.exception_type is CalendarException.Type.ADDED:
                self.calendar_by_day[exception.date].add(exception.calendar_id)
            else:
                self.calendar_by_day[exception.date].discard(exception.calendar_id)

    def compute_coverages(self) -> None:
        self.current_coverage = BoundedDateRange(
            min(self.calendar_by_day), max(self.calendar_by_day)
        )

        expected_start_date = self.start_date or self.current_coverage.start
        self.expected_coverage = BoundedDateRange(
            expected_start_date,
            expected_start_date.add_days(self.duration_days),
        )

    def is_extension_necessary(self) -> bool:
        return not self.expected_coverage.issubset(self.current_coverage)

    def update_calendars(self, db: DBConnection) -> None:
        with db.transaction():
            # Set all calendars to use exceptions
            db.raw_execute(
                "UPDATE calendars SET monday = 0, tuesday = 0, wednesday = 0, thursday = 0, "
                "friday = 0, saturday = 0, sunday = 0, "
                "start_date = '1111-11-11', end_date = '1111-11-11'"
            )

            # Remove any existing exceptions
            db.raw_execute("DELETE FROM calendar_exceptions")

            # Re-create the calendar exceptions
            db.raw_execute_many(
                "INSERT INTO calendar_exceptions (date,calendar_id,exception_type) VALUES (?,?,1)",
                (
                    (day.isoformat(), calendar_id)
                    for day, calendar_ids in self.calendar_by_day.items()
                    for calendar_id in calendar_ids
                ),
            )

    def extend_calendars(self) -> None:
        for day in self.expected_coverage:
            if day not in self.calendar_by_day:
                weekday = 6 if self.is_holiday(day) else day.weekday()
                src_day = self.extend_template[weekday]
                self.logger.debug("Copying %s calendars to %s", src_day, day)
                if src_day is not None:
                    self.calendar_by_day[day] = self.calendar_by_day[src_day].copy()

    def is_holiday(self, day: Date) -> bool:
        return day in self.holidays


class ExtendCalendarsFromPolishExceptions(ExtendCalendars):
    """ExtendCalendarsFromPolishExceptions is an extension of :py:class:`ExtendCalendars`
    which lazily-loads holidays from :py:attr:`impuls.tools.polish_calendar_exceptions.RESOURCE`.
    """

    def __init__(
        self,
        resource_name: str,
        region: polish_calendar_exceptions.PolishRegion,
        duration_days: int = 30,
        start_date: Date | None = None,
        fallback_weekdays: Sequence[Sequence[int]] = ExtendCalendars.DEFAULT_FALLBACK_WEEKDAYS,
    ) -> None:
        super().__init__(duration_days, start_date, fallback_weekdays)
        self.resource_name = resource_name
        self.region = region

    def execute(self, r: TaskRuntime) -> None:
        self.load_holidays(r.resources[self.resource_name])
        return super().execute(r)

    def load_holidays(self, r: ManagedResource) -> None:
        assert isinstance(self.holidays, set)
        exceptions = polish_calendar_exceptions.load_exceptions(r, self.region)
        for day, exception in exceptions.items():
            if polish_calendar_exceptions.CalendarExceptionType.HOLIDAY in exception.typ:
                self.holidays.add(day)


def _max_date(a: Date, b: Date | None) -> Date:
    return max(a, b) if b else a


def _fake_date_range() -> BoundedDateRange:
    return BoundedDateRange(Date(1, 1, 1), Date(1, 1, 1))
