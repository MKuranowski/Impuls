import re
from datetime import date, timedelta
from typing import ClassVar, Type

from ...tools.types import Self


class TimePoint(timedelta):
    """TimePoint is an extension of datetime.timedelta to represent
    *seconds since noon minus 12 hours*.

    The extension only provides ``__str__`` and :py:meth:`from_str` methods
    to help with conversion between TimePoints and HH:MM:SS strings.
    """

    def __str__(self) -> str:
        """Converts the TimePoint to a GTFS-compliant string

        >>> str(TimePoint(hours=8, minutes=30, seconds=0))
        '08:30:00'
        >>> str(TimePoint(hours=25, minutes=1, seconds=8))
        '25:01:08'
        """
        m, s = divmod(int(self.total_seconds()), 60)
        h, m = divmod(m, 60)
        return f"{h:0>2}:{m:0>2}:{s:0>2}"

    @classmethod
    def from_str(cls: Type[Self], x: str) -> Self:
        """Parses a TimePoint from a HH:MM:SS strings

        >>> TimePoint.from_str("8:30:00").total_seconds()
        30600.0
        >>> TimePoint.from_str("08:30:00").total_seconds()
        30600.0
        >>> TimePoint.from_str("25:01:08").total_seconds()
        90068.0
        """
        h, m, s = map(int, x.split(":"))
        return cls(seconds=h * 3600 + m * 60 + s)


class Date(date):
    """Date is an extension of datetime.date to represent dates of the Impuls model.

    The extension provides ``__str__`` and :py:meth:`from_ymd_str` methods to convert
    between dates and YYYY-MM-DD strings, and a few other helper functions.
    """

    SIGNALS_EXCEPTIONS: ClassVar["Date"]
    """A placeholder Date used in :py:class:`~impuls.model.Calendar` to indicate that
    this calendar is defined exclusively using :py:class:`~impuls.model.CalendarException`
    instances. In principle, this could be any date, but only :py:obj:`SIGNALS_EXCEPTIONS`
    is identified by the :py:class:`~impuls.tasks.SaveGTFS` task.
    """

    def __str__(self) -> str:
        """Converts the string to a YYYY-MM-DD format.

        >>> str(Date(2012, 6, 1))
        '2012-06-01'
        """
        return self.strftime("%Y-%m-%d")

    @classmethod
    def from_ymd_str(cls: Type[Self], x: str) -> Self:
        """Parses a YYYY-MM-DD string into a Date.
        The separator may be omitted, or may be any non-word characters.

        >>> Date.from_ymd_str("2012-06-01")
        Date(2012, 6, 1)
        >>> Date.from_ymd_str("20120825")
        Date(2012, 8, 25)
        >>> Date.from_ymd_str("2012.02.29")
        Date(2012, 2, 29)
        """
        m = re.fullmatch(r"([0-9]{1,4})\W?([0-9]{1,2})\W?([0-9]{1,2})", x)
        if not m:
            raise ValueError(f"invalid year-month-date string: {x!r}")
        return cls(int(m[1]), int(m[2]), int(m[3]))

    def add_days(self: Self, delta: int) -> Self:
        """Returns a new day ``delta`` days off from self. Delta may be negative.

        >>> Date(2012, 6, 1).add_days(4)
        Date(2012, 6, 5)
        >>> Date(2012, 12, 27).add_days(7)
        Date(2013, 1, 3)
        >>> Date(2012, 2, 28).add_days(1)
        Date(2012, 2, 29)
        >>> Date(2013, 2, 28).add_days(1)
        Date(2013, 3, 1)
        """
        return self + timedelta(days=delta)


Date.SIGNALS_EXCEPTIONS = Date(1111, 11, 11)
