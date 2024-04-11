import ctypes
import logging
import os
import sys
from ctypes import CFUNCTYPE, POINTER, c_bool, c_char_p, c_int
from pathlib import Path
from typing import Mapping, Sequence

from ..tools.types import StrPath

logger = logging.getLogger(__name__)

c_char_p_p = POINTER(c_char_p)

if sys.platform.startswith("win32"):
    lib_filename = "libextern.dll"
elif sys.platform.startswith("darwin"):
    lib_filename = "libextern.dylib"
else:
    lib_filename = "libextern.so"

lib_path = Path(__file__).with_name(lib_filename)
lib = ctypes.cdll.LoadLibrary(str(lib_path))


# XXX: The declarations below must match zig/lib.zig


class _GTFSHeaders(ctypes.Structure):
    _fields_ = [
        ("agency", c_char_p_p),
        ("attributions", c_char_p_p),
        ("calendar", c_char_p_p),
        ("calendar_dates", c_char_p_p),
        ("feed_info", c_char_p_p),
        ("routes", c_char_p_p),
        ("stops", c_char_p_p),
        ("shapes", c_char_p_p),
        ("trips", c_char_p_p),
        ("stop_times", c_char_p_p),
        ("frequencies", c_char_p_p),
        ("transfers", c_char_p_p),
        ("fare_attributes", c_char_p_p),
        ("fare_rules", c_char_p_p),
    ]


_LogHandler = CFUNCTYPE(None, c_int, c_char_p)


lib.load_gtfs.argtypes = [_LogHandler, c_char_p, c_char_p]
lib.load_gtfs.restype = c_int

lib.save_gtfs.argtypes = [_LogHandler, c_char_p, c_char_p, POINTER(_GTFSHeaders), c_bool]
lib.save_gtfs.restype = c_int


def load_gtfs(db_path: StrPath, gtfs_dir_path: StrPath) -> None:
    log_handler = _LogHandler(logger.log)
    status: int = lib.load_gtfs(
        log_handler,
        os.fspath(db_path).encode("utf-8"),
        os.fspath(gtfs_dir_path).encode("utf-8"),
    )
    if status:
        raise RuntimeError(f"extern load_gtfs failed with {status}")


def save_gtfs(
    db_path: StrPath,
    gtfs_dir_path: StrPath,
    headers: Mapping[str, Sequence[str]],
    emit_empty_calendars: bool = False,
) -> None:
    extern_headers = _GTFSHeaders()
    for file, header in headers.items():
        if not header:
            continue
        extern_header = (c_char_p * (len(header) + 1))()
        for i, field in enumerate(header):
            extern_header[i] = field.encode("utf-8")
        setattr(extern_headers, file, extern_header)

    log_handler = _LogHandler(logger.log)
    status: int = lib.save_gtfs(
        log_handler,
        os.fspath(db_path).encode("utf-8"),
        os.fspath(gtfs_dir_path).encode("utf-8"),
        ctypes.byref(extern_headers),
        emit_empty_calendars,
    )
    if status:
        raise RuntimeError(f"extern load_gtfs failed with {status}")
