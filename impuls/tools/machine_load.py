import resource
import sys
from time import perf_counter
from typing import Any

from .types import Self


def memory_usage_kb() -> int:
    """Returns the memory usage of the current process.

    On POSIX systems (incl. Linux, BSD and MacOS) returns
    the maximum resident set size.

    On Windows always returns zero - as this method is not implemented.
    """
    if sys.platform == "win32":
        # FIXME: Find an equivalent Windows system call
        return 0

    usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss

    if sys.platform == "darwin":
        # NOTE: AFAIK MacOS is the only os which returns bytes instead of kilobytes
        usage //= 1024

    return usage


class LoadTracker:
    """LoadTracker is a simple context manager that tracks time and memory usage
    of the `with` body.

    >>> with LoadTracker() as load:
    ...    # Some expensive operation (here: simple primality test)
    ...    n = 10358100653869
    ...    all(n % i != 0 for i in range(2, int(n ** .5) + 2))
    True
    >>> 0.0 < load.delta_time < 1.0
    True
    """

    def __init__(self) -> None:
        self.start_time: float = float("nan")
        self.start_rss: int = -1
        self.end_time: float = float("nan")
        self.end_rss: int = -1

    @property
    def delta_time(self) -> float:
        return self.end_time - self.start_time

    @property
    def delta_rss(self) -> int:
        return self.end_rss - self.start_rss

    def __enter__(self: Self) -> Self:
        self.start_time = perf_counter()
        self.start_rss = memory_usage_kb()
        return self

    def __exit__(self, *_: Any) -> None:
        self.end_time = perf_counter()
        self.end_rss = memory_usage_kb()

    def __str__(self) -> str:
        return (
            f"elapsed: {self.delta_time:.3f} s; memory usage: {self.start_rss // 1024} MiB → "
            f"{self.end_rss // 1024} MiB (diff {self.delta_rss} KiB)"
        )
