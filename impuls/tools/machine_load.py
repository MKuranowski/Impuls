import sys
from time import perf_counter
from typing import Any

from .types import Self


if sys.platform == "win32":
    import ctypes
    import ctypes.wintypes
    kernel32 = ctypes.windll.kernel32

    class ProcessMemoryCounters(ctypes.Structure):
        # Source: https://learn.microsoft.com/en-us/windows/win32/api/psapi/ns-psapi-process_memory_counters  # noqa: E501

        _fields_ = [
            ("cb", ctypes.wintypes.DWORD),
            ("PageFaultCount", ctypes.wintypes.DWORD),
            ("PeakWorkingSetSize", ctypes.c_size_t),
            ("WorkingSetSize", ctypes.c_size_t),
            ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
            ("QuotaPagedPoolUsage", ctypes.c_size_t),
            ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
            ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
            ("PagefileUsage", ctypes.c_size_t),
            ("PeakPagefileUsage", ctypes.c_size_t),
        ]

    def memory_usage_kb() -> int:
        """Returns the memory usage of the current process."""
        process_handle = kernel32.GetCurrentProcess()
        try:
            # Use the GetProcessMemoryInfo function to retrieve memory usage.
            # https://learn.microsoft.com/en-us/windows/win32/api/psapi/nf-psapi-getprocessmemoryinfo
            mem = ProcessMemoryCounters()
            if not kernel32.GetProcessMemoryInfo(
                process_handle, ctypes.byref(mem), ctypes.sizeof(mem)
            ):
                raise ctypes.WinError()
            assert int(mem.cb) == ctypes.sizeof(mem)

            # PeakWorkingSetSize should be the closest to POSIX max resident set size.
            # PeakWorkingSetSize is provided in bytes
            return int(mem.PeakWorkingSetSize) // 1024
        finally:
            kernel32.CloseHandle(process_handle)

elif sys.platform == "darwin":
    import resource

    def memory_usage_kb() -> int:
        """Returns the memory usage of the current process."""
        # NOTE: Darwin returns the number in bytes, not KiB
        return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss // 1024

else:
    import resource

    def memory_usage_kb() -> int:
        """Returns the memory usage of the current process."""
        return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss


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
