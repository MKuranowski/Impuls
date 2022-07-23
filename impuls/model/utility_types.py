from datetime import date, timedelta
from enum import IntEnum
from typing import ClassVar


class TimePoint(timedelta):
    def __str__(self) -> str:
        m, s = divmod(int(self.total_seconds()), 60)
        h, m = divmod(m, 60)
        return f"{h:0>2}:{m:0>2}:{s:0>2}"

    @classmethod
    def from_str(cls, x: str):
        h, m, s = map(int, x.split(":"))
        return cls(seconds=h * 3600 + m * 60 + s)


class Date(date):
    SIGNALS_EXCEPTIONS: ClassVar["Date"]


Date.SIGNALS_EXCEPTIONS = Date(1111, 11, 11)


class Maybe(IntEnum):
    """Maybe is a representation of GTFS tri-state fields.

    Maybe differs from `Optional[bool]` in the way the false and unknown states
    are encoded in GTFS:

    | State description | Optional[bool] (its GTFS repr.) | Maybe (its GTFS repr.) |
    |-------------------|---------------------------------|------------------------|
    | Unknown (default) | None ("")                       | UNKNOWN ("0")          |
    | True              | True ("1")                      | YES ("1")              |
    | False             | False ("0")                     | NO ("2")               |
    """

    UNKNOWN = 0
    YES = 1
    NO = 2
