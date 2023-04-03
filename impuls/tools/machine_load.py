import sys
from time import perf_counter
from typing import Any

from .types import Self


if sys.platform == "win32":
    from ctypes import wintypes
    import ctypes

    # cSpell: words psapi
    kernel32 = ctypes.windll.kernel32
    psapi = ctypes.windll.psapi

    class ProcessMemoryCounters(ctypes.Structure):
        # https://learn.microsoft.com/en-us/windows/win32/api/psapi/ns-psapi-process_memory_counters
        _fields_ = [
            ("cb", wintypes.DWORD),
            ("PageFaultCount", wintypes.DWORD),
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
        process_handle = wintypes.HANDLE(kernel32.GetCurrentProcess())
        try:
            # Use the GetProcessMemoryInfo function to retrieve memory usage.
            # https://learn.microsoft.com/en-us/windows/win32/api/psapi/nf-psapi-getprocessmemoryinfo
            pmc = ProcessMemoryCounters(cb=ctypes.sizeof(ProcessMemoryCounters))
            if not psapi.GetProcessMemoryInfo(process_handle, ctypes.byref(pmc), pmc.cb):
                raise ctypes.WinError()

            # PeakWorkingSetSize is in bytes
            return int(pmc.PeakWorkingSetSize) // 1024
        finally:
            # Ensure the process handle is closed; although the Windows docs day
            # that the CloseHandle on current process handle does nothing.
            if not kernel32.CloseHandle(process_handle):
                raise ctypes.WinError()

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
