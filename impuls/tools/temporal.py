# © Copyright 2022-2025 Mikołaj Kuranowski
# SPDX-License-Identifier: GPL-3.0-or-later

from abc import ABC, abstractmethod
from datetime import date
from math import inf
from typing import Any, Iterator, Union, final, overload

from ..model.meta.utility_types import Date

DateRange = Union[
    "EmptyDateRange",
    "InfiniteDateRange",
    "LeftUnboundedDateRange",
    "RightUnboundedDateRange",
    "BoundedDateRange",
]
"""DateRange is any type representing a contiguous range of dates.

Infinite, LeftUnbounded and RightUnbounded ranges cover infinitely many days.
"""


class _DateRangeABC(ABC):
    @property
    @abstractmethod
    def compressed_weekdays(self) -> int:
        """Bitset of weekdays covered by the DateRange.
        `1 << 0` is Monday, `1 << 1` is Tuesday and so on, up to Sunday, `1 << 6`.

        Always `0b111_1111` (all weekdays) for unbounded DateRanges.
        """
        ...

    @abstractmethod
    def len(self) -> float:
        """Returns the number of days covered by the DateRange.
        Always a non-negative integer, or `inf` (for unbounded ranges).
        """
        ...

    @abstractmethod
    def __contains__(self, x: Date) -> bool:
        """Checks if Date is contained within a DateRange."""
        ...

    @abstractmethod
    def __iter__(self) -> Iterator[Date]:
        """Iterates over all dates covered by a DateRange.

        For LeftUnbounded and RightUnbounded ranges the iterator will be infinite.
        Trying to iterate over InfiniteDateRange throws RuntimeError.
        """
        ...

    @abstractmethod
    def __hash__(self) -> int: ...

    @abstractmethod
    def __eq__(self, o: Any) -> bool: ...

    @abstractmethod
    def isdisjoint(self, o: "DateRange") -> bool:
        """Returns True if `self` and `o` have no dates in common."""
        ...

    @abstractmethod
    def issubset(self, o: "DateRange") -> bool:
        """Returns True if all dates covered by `self` are also covered by `o`."""
        ...

    @abstractmethod
    def union(self, o: "DateRange") -> "DateRange":
        """Returns a DateRange which covers dates that are either in `self` or `o`.
        Raises ArithmeticError if union of ranges would not be contiguous.
        """
        ...

    def __or__(self, o: "DateRange") -> "DateRange":
        """Returns a DateRange which covers dates that are either in `self` or `o`.
        Raises ArithmeticError if union of ranges would not be contiguous.
        """
        return self.union(o)

    @abstractmethod
    def intersection(self, o: "DateRange") -> "DateRange":
        """Returns a DateRange which covers dates from `self` that are also covered in `o`."""
        ...

    def __and__(self, o: "DateRange") -> "DateRange":
        """Returns a DateRange which covers dates from `self` that are also covered in `o`."""
        return self.intersection(o)

    @abstractmethod
    def difference(self, o: "DateRange") -> "DateRange":
        """Returns a DateRange which covers dates that are in `self`, but not in `o`.
        Raises ArithmeticError if difference of ranges would not be contiguous.
        """
        ...

    def __sub__(self, o: "DateRange") -> "DateRange":
        """Returns a DateRange which covers dates that are in `self`, but not in `o`.
        Raises ArithmeticError if difference of ranges would not be contiguous.
        """
        return self.difference(o)


@final
class EmptyDateRange(_DateRangeABC):
    """EmptyDateRange is a range of dates without any dates."""

    def __repr__(self) -> str:
        return "EmptyDateRange()"

    @property
    def compressed_weekdays(self) -> int:
        return 0

    def __contains__(self, x: Date) -> bool:
        return False

    def len(self) -> float:
        return 0

    def __iter__(self) -> Iterator[Date]:
        yield from tuple()

    def __hash__(self) -> int:
        return hash(None)

    def __eq__(self, o: Any) -> bool:
        return isinstance(o, EmptyDateRange)

    def isdisjoint(self, o: DateRange) -> bool:
        return True

    def issubset(self, o: DateRange) -> bool:
        return True

    def union(self, o: DateRange) -> DateRange:
        return o

    def intersection(self, o: DateRange) -> DateRange:
        return self

    def difference(self, o: DateRange) -> DateRange:
        return self


@final
class InfiniteDateRange(_DateRangeABC):
    """InfiniteDateRange is a range of dates covering every date."""

    def __repr__(self) -> str:
        return "InfiniteDateRange()"

    @property
    def compressed_weekdays(self) -> int:
        return 0b111_1111

    def __contains__(self, x: Date) -> bool:
        return True

    def len(self) -> float:
        return inf

    def __iter__(self) -> Iterator[Date]:
        raise RuntimeError("Can't iterate over InfiniteDateRange")

    def __hash__(self) -> int:
        return hash(None)

    def __eq__(self, o: Any) -> bool:
        return isinstance(o, InfiniteDateRange)

    def isdisjoint(self, o: DateRange) -> bool:
        return isinstance(o, EmptyDateRange)

    def issubset(self, o: DateRange) -> bool:
        return isinstance(o, InfiniteDateRange)

    def union(self, o: DateRange) -> DateRange:
        return self

    def intersection(self, o: DateRange) -> DateRange:
        return o

    def difference(self, o: DateRange) -> DateRange:
        match o:
            case EmptyDateRange():
                return self
            case InfiniteDateRange():
                return EmptyDateRange()
            case LeftUnboundedDateRange():
                return RightUnboundedDateRange(start=o.end.add_days(1))
            case RightUnboundedDateRange():
                return LeftUnboundedDateRange(end=o.start.add_days(-1))
            case BoundedDateRange():
                raise ArithmeticError(f"{self} - {o} creates DateRange with holes")


@final
class LeftUnboundedDateRange(_DateRangeABC):
    """LeftUnboundedDateRange is a range of all dates up to (and including) the end date."""

    def __init__(self, end: Date) -> None:
        self.end = end

    def __repr__(self) -> str:
        return f"date_range(None, {self.end!r})"

    @property
    def compressed_weekdays(self) -> int:
        return 0b111_1111

    def __contains__(self, x: Date) -> bool:
        return x <= self.end

    def len(self) -> float:
        return inf

    def __iter__(self) -> Iterator[Date]:
        d = self.end
        while True:
            yield d
            d = d.add_days(-1)

    def __hash__(self) -> int:
        return hash((None, self.end))

    def __eq__(self, o: Any) -> bool:
        return isinstance(o, LeftUnboundedDateRange) and self.end == o.end

    def isdisjoint(self, o: DateRange) -> bool:
        match o:
            case EmptyDateRange():
                return True
            case LeftUnboundedDateRange() | InfiniteDateRange():
                return False
            case RightUnboundedDateRange() | BoundedDateRange():
                return o.start > self.end

    def issubset(self, o: DateRange) -> bool:
        match o:
            case EmptyDateRange() | RightUnboundedDateRange() | BoundedDateRange():
                return False
            case InfiniteDateRange():
                return True
            case LeftUnboundedDateRange():
                return self.end <= o.end

    def union(self, o: DateRange) -> DateRange:
        match o:
            case EmptyDateRange():
                return self
            case InfiniteDateRange():
                return o
            case LeftUnboundedDateRange():
                return LeftUnboundedDateRange(end=max(self.end, o.end))
            case RightUnboundedDateRange():
                if o.start > self.end.add_days(1):
                    raise ArithmeticError(f"{self} | {o} creates DateRange with holes")
                return InfiniteDateRange()
            case BoundedDateRange():
                if o.start > self.end.add_days(1):
                    raise ArithmeticError(f"{self} | {o} creates DateRange with holes")
                return LeftUnboundedDateRange(end=max(self.end, o.end))

    def intersection(self, o: DateRange) -> DateRange:
        match o:
            case EmptyDateRange():
                return o
            case InfiniteDateRange():
                return self
            case LeftUnboundedDateRange():
                return LeftUnboundedDateRange(end=min(self.end, o.end))
            case RightUnboundedDateRange():
                if o.start > self.end:
                    return EmptyDateRange()
                return BoundedDateRange(o.start, self.end)
            case BoundedDateRange():
                if o.start > self.end:
                    return EmptyDateRange()
                return BoundedDateRange(start=o.start, end=min(self.end, o.end))

    def difference(self, o: DateRange) -> DateRange:
        match o:
            case EmptyDateRange():
                return self
            case InfiniteDateRange():
                return EmptyDateRange()
            case LeftUnboundedDateRange():
                if o.end < self.end:
                    return BoundedDateRange(o.end.add_days(1), self.end)
                return EmptyDateRange()
            case RightUnboundedDateRange():
                if o.start <= self.end:
                    return LeftUnboundedDateRange(end=o.start.add_days(-1))
                return self
            case BoundedDateRange():
                if o.end < self.end:
                    raise ArithmeticError(f"{self} - {o} creates DateRange with holes")
                return LeftUnboundedDateRange(end=min(self.end, o.start.add_days(-1)))


@final
class RightUnboundedDateRange(_DateRangeABC):
    """RightUnboundedDateRange is a range of all dates starting from the start date."""

    def __init__(self, start: Date) -> None:
        self.start = start

    def __repr__(self) -> str:
        return f"date_range({self.start!r}, None)"

    @property
    def compressed_weekdays(self) -> int:
        return 0b111_1111

    def __contains__(self, x: Date) -> bool:
        return x >= self.start

    def len(self) -> float:
        return inf

    def __iter__(self) -> Iterator[Date]:
        d = self.start
        while True:
            yield d
            d = d.add_days(1)

    def __hash__(self) -> int:
        return hash((self.start, None))

    def __eq__(self, o: Any) -> bool:
        return isinstance(o, RightUnboundedDateRange) and o.start == self.start

    def isdisjoint(self, o: DateRange) -> bool:
        match o:
            case EmptyDateRange():
                return True
            case InfiniteDateRange() | RightUnboundedDateRange():
                return False
            case LeftUnboundedDateRange() | BoundedDateRange():
                return o.end < self.start

    def issubset(self, o: DateRange) -> bool:
        match o:
            case EmptyDateRange() | LeftUnboundedDateRange() | BoundedDateRange():
                return False
            case InfiniteDateRange():
                return True
            case RightUnboundedDateRange():
                return self.start >= o.start

    def union(self, o: DateRange) -> DateRange:
        match o:
            case EmptyDateRange():
                return self
            case InfiniteDateRange():
                return o
            case RightUnboundedDateRange():
                return RightUnboundedDateRange(start=min(self.start, o.start))
            case LeftUnboundedDateRange():
                if self.start > o.end.add_days(1):
                    raise ArithmeticError(f"{self} | {o} creates DateRange with holes")
                return InfiniteDateRange()
            case BoundedDateRange():
                if o.end.add_days(1) < self.start:
                    raise ArithmeticError(f"{self} | {o} creates DateRange with holes")
                return RightUnboundedDateRange(start=min(self.start, o.start))

    def intersection(self, o: DateRange) -> DateRange:
        match o:
            case EmptyDateRange():
                return o
            case InfiniteDateRange():
                return self
            case RightUnboundedDateRange():
                return RightUnboundedDateRange(start=max(self.start, o.start))
            case LeftUnboundedDateRange():
                if self.start > o.end:
                    return EmptyDateRange()
                return BoundedDateRange(self.start, o.end)
            case BoundedDateRange():
                if self.start > o.end:
                    return EmptyDateRange()
                return BoundedDateRange(start=max(self.start, o.start), end=o.end)

    def difference(self, o: DateRange) -> DateRange:
        match o:
            case EmptyDateRange():
                return self
            case InfiniteDateRange():
                return EmptyDateRange()
            case RightUnboundedDateRange():
                if o.start > self.start:
                    return BoundedDateRange(self.start, o.start.add_days(-1))
                return EmptyDateRange()
            case LeftUnboundedDateRange():
                if o.end >= self.start:
                    return RightUnboundedDateRange(start=o.end.add_days(1))
                return self
            case BoundedDateRange():
                if o.start > self.start:
                    raise ArithmeticError(f"{self} - {o} creates DateRange with holes")
                return RightUnboundedDateRange(start=max(self.start, o.end.add_days(1)))


@final
class BoundedDateRange(_DateRangeABC):
    """RightUnboundedDateRange is a range of all dates between start and end, inclusive."""

    def __init__(self, start: Date, end: Date) -> None:
        self.start = start
        self.end = end
        if self.start > self.end:
            raise ValueError(f"Invalid DateRange: {start.isoformat()} ~ {end.isoformat()}")

        self._cached_len: int | None = None
        self._cached_compressed_weekdays: int | None = None

    def __repr__(self) -> str:
        return f"date_range({self.start!r}, {self.end!r})"

    @property
    def compressed_weekdays(self) -> int:
        if self._cached_compressed_weekdays is None:
            if self.len() >= 7:
                self._cached_compressed_weekdays = 0b111_1111
            else:
                # Manually compute the set of weekdays
                compressed_weekdays = 0
                for d in self:
                    compressed_weekdays |= 1 << d.weekday()
                return compressed_weekdays
        return self._cached_compressed_weekdays

    def __contains__(self, x: Date) -> bool:
        return self.start <= x <= self.end

    def len(self) -> float:
        if self._cached_len is None:
            self._cached_len = (self.end - self.start).days + 1
        return self._cached_len

    def __iter__(self) -> Iterator[Date]:
        d = self.start
        while d <= self.end:
            yield d
            d = d.add_days(1)

    def __hash__(self) -> int:
        return hash((self.start, self.end))

    def __eq__(self, o: Any) -> bool:
        return isinstance(o, BoundedDateRange) and o.start == self.start and o.end == self.end

    def isdisjoint(self, o: DateRange) -> bool:
        match o:
            case EmptyDateRange():
                return True
            case InfiniteDateRange():
                return False
            case LeftUnboundedDateRange():
                return self.start > o.end
            case RightUnboundedDateRange():
                return self.end < o.start
            case BoundedDateRange():
                return self.start > o.end or self.end < o.start

    def issubset(self, o: DateRange) -> bool:
        match o:
            case EmptyDateRange():
                return False
            case InfiniteDateRange():
                return True
            case LeftUnboundedDateRange():
                return self.end <= o.end
            case RightUnboundedDateRange():
                return self.start >= o.start
            case BoundedDateRange():
                return self.start >= o.start and self.end <= o.end

    def union(self, o: DateRange) -> DateRange:
        match o:
            case EmptyDateRange():
                return self
            case InfiniteDateRange():
                return o
            case LeftUnboundedDateRange():
                if self.start > o.end.add_days(1):
                    raise ArithmeticError(f"{self} | {o} creates DateRange with holes")
                return LeftUnboundedDateRange(end=max(self.end, o.end))
            case RightUnboundedDateRange():
                if self.end.add_days(1) < o.start:
                    raise ArithmeticError(f"{self} | {o} creates DateRange with holes")
                return RightUnboundedDateRange(start=min(self.start, o.start))
            case BoundedDateRange():
                if self.start > o.end.add_days(1) or self.end.add_days(1) < o.start:
                    raise ArithmeticError(f"{self} | {o} creates DateRange with holes")
                return BoundedDateRange(start=min(self.start, o.start), end=max(self.end, o.end))

    def intersection(self, o: DateRange) -> DateRange:
        match o:
            case EmptyDateRange():
                return o
            case InfiniteDateRange():
                return self
            case LeftUnboundedDateRange():
                if self.start > o.end.add_days(1):
                    return EmptyDateRange()
                return BoundedDateRange(start=self.start, end=min(self.end, o.end))
            case RightUnboundedDateRange():
                if self.end.add_days(1) < self.start:
                    return EmptyDateRange()
                return BoundedDateRange(start=max(self.start, o.start), end=self.end)
            case BoundedDateRange():
                if self.start > o.end or self.end < o.start:
                    return EmptyDateRange()
                return BoundedDateRange(start=max(self.start, o.start), end=min(self.end, o.end))

    def difference(self, o: DateRange) -> DateRange:
        match o:
            case EmptyDateRange():
                return self
            case InfiniteDateRange():
                return EmptyDateRange()
            case LeftUnboundedDateRange():
                if o.end < self.start:
                    return self
                elif o.end < self.end:
                    return BoundedDateRange(start=o.end.add_days(1), end=self.end)
                else:
                    return EmptyDateRange()
            case RightUnboundedDateRange():
                if o.start > self.end:
                    return self
                elif o.start > self.start:
                    return BoundedDateRange(start=self.start, end=o.start.add_days(-1))
                else:
                    return EmptyDateRange()
            case BoundedDateRange():
                if o.start > self.end or o.end < self.start:
                    return self
                elif o.start > self.start and o.end < self.end:
                    raise ArithmeticError(f"{self} - {o} creates DateRange with holes")
                elif o.start <= self.start and o.end >= self.end:
                    return EmptyDateRange()
                elif o.start <= self.start:
                    return BoundedDateRange(start=o.end.add_days(1), end=self.end)
                elif o.end >= self.end:
                    return BoundedDateRange(start=self.start, end=o.start.add_days(-1))
                else:
                    raise RuntimeError("unreachable code")


@overload
def date_range(start: Date, end: Date) -> BoundedDateRange: ...


@overload
def date_range(start: Date, end: None = None) -> RightUnboundedDateRange: ...


@overload
def date_range(start: None, end: Date) -> LeftUnboundedDateRange: ...


def date_range(start: Date | None, end: Date | None = None) -> DateRange:
    """date_range returns a DateRange object for all dates from
    ``start`` to ``end``, inclusive.

    Those objects can be iterated over, but also combined using set-like operations
    with the following methods:

    * isdisjoint
    * issubset (operator ``<``)
    * union (operator ``|``)
    * intersection (operator ``&``)
    * difference (operator ``-``)
    * (operator ``==``)

    :py:obj:`DateRange` instances are hashable, and can be used as dictionary keys.
    DateRange objects are also iterable (with the exception of :py:class:`InfiniteDateRange`),
    but iterators with start=None or end=None are infinite.

    If ``start`` is None, returns :py:class:`LeftUnboundedDateRange` -
    a DateRange without a start bound.

    If ``end`` is None, returns :py:class:`RightUnboundedDateRange` -
    a DateRange without an end bound.

    However, if both ``start`` and ``end`` are None - an exception is raised,
    please explicitly construct :py:class:`EmptyDateRange` or :py:class:`InfiniteDateRange`.
    """
    if start and end:
        return BoundedDateRange(start, end)
    elif start:
        return RightUnboundedDateRange(start)
    elif end:
        return LeftUnboundedDateRange(end)
    else:
        raise ValueError(
            "date_range(None, None) is ambiguous - "
            "use EmptyDateRange() or InfiniteDateRange() explicitly"
        )


def get_european_railway_schedule_revision(for_day: date | None = None) -> str:
    """Gets the name of the yearly European railway schedule revision active
    on the provided day, or today if that is missing.

    The yearly schedule revision changes on midnight after (think *24:00*) the 2nd Saturday
    of december. This means that for the 2nd Saturday of December we actually return the old
    revision, as the new schedules only start applying from the following Sunday.

    The returned string is the year the revision went live, dash, then the following year.

    >>> get_european_railway_schedule_revision(date(2025, 12, 1))
    '2024-2025'
    >>> get_european_railway_schedule_revision(date(2025, 12, 13))
    '2024-2025'
    >>> get_european_railway_schedule_revision(date(2025, 12, 14))
    '2025-2026'
    >>> get_european_railway_schedule_revision(date(2025, 12, 31))
    '2025-2026'
    >>> get_european_railway_schedule_revision(date(2025, 12, 31))
    '2025-2026'
    >>> get_european_railway_schedule_revision(date(2024, 12, 14))
    '2023-2024'
    >>> get_european_railway_schedule_revision(date(2024, 12, 15))
    '2024-2025'
    """
    for_day = for_day or date.today()
    base_year = for_day.year - 1
    if for_day.month == 12:
        # Calculate the change date - the day after the 2nd Saturday of december
        dec_1 = Date(for_day.year, 12, 1)
        dec_1st_sat = (5 - dec_1.weekday()) % 7
        delta_days = dec_1st_sat + 8
        change_day = dec_1.add_days(delta_days)
        if for_day >= change_day:
            base_year = for_day.year

    return f"{base_year}-{base_year + 1}"
