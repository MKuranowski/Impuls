from typing import Iterable

from ..model.utility_types import Date


def date_range(start: Date, end: Date | None = None) -> Iterable[Date]:
    """date_range returns a generator of all dates from
    `start` to `end`, inclusive.
    If `end` is None, returns infinite number of days.

    >>> list(date_range(Date(2012, 6, 1), Date(2012, 6, 3)))
    [Date(2012, 6, 1), Date(2012, 6, 2), Date(2012, 6, 3)]
    >>> list(date_range(Date(2012, 2, 28), Date(2012, 3, 1)))
    [Date(2012, 2, 28), Date(2012, 2, 29), Date(2012, 3, 1)]
    """
    while end is None or start <= end:
        yield start
        start = start.add_days(1)
