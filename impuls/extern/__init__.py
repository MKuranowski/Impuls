# © Copyright 2022-2025 Mikołaj Kuranowski
# SPDX-License-Identifier: GPL-3.0-or-later

import ctypes
import logging
import os
import sys
from ctypes import CFUNCTYPE, POINTER, c_bool, c_char_p, c_int, c_uint
from pathlib import Path
from typing import Mapping, Sequence

from ..tools.types import StrPath

__all__ = ["load_gtfs", "save_gtfs"]

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


class _FileHeader(ctypes.Structure):
    _fields_ = [
        ("file_name", c_char_p),
        ("fields", c_char_p_p),
    ]


_LogHandler = CFUNCTYPE(None, c_int, c_char_p)


@_LogHandler
def _log_handler(level: int, msg: bytes) -> None:
    logger.log(level, msg.decode("utf-8"))


lib.set_log_handler.argtypes = [_LogHandler]
lib.set_log_handler.restype = None
lib.set_log_handler(_log_handler)

lib.load_gtfs.argtypes = [c_char_p, c_char_p, c_bool, c_char_p_p, c_uint]
lib.load_gtfs.restype = c_int

lib.save_gtfs.argtypes = [c_char_p, c_char_p, POINTER(_FileHeader), c_int, c_bool, c_bool]
lib.save_gtfs.restype = c_int


def load_gtfs(
    db_path: StrPath,
    gtfs_dir_path: StrPath,
    extra_fields: bool = False,
    extra_files: Sequence[str] = [],
) -> None:
    extra_files_encoded = (c_char_p * len(extra_files))()
    for i, extra_file in enumerate(extra_files):
        extra_files_encoded[i] = extra_file.encode("utf-8")

    status: int = lib.load_gtfs(
        os.fspath(db_path).encode("utf-8"),
        os.fspath(gtfs_dir_path).encode("utf-8"),
        extra_fields,
        extra_files_encoded,
        len(extra_files),
    )
    if status:
        raise RuntimeError(f"extern load_gtfs failed with {status}")


def save_gtfs(
    db_path: StrPath,
    gtfs_dir_path: StrPath,
    headers: Mapping[str, Sequence[str]],
    emit_empty_calendars: bool = False,
    ensure_order: bool = False,
) -> None:
    extern_headers = (_FileHeader * len(headers))()
    for i, (file_name, field_names) in enumerate(headers.items()):
        extern_header = (c_char_p * (len(field_names) + 1))()
        for j, field in enumerate(field_names):
            extern_header[j] = field.encode("utf-8")
        extern_headers[i] = _FileHeader(file_name.encode("utf-8"), extern_header)

    status: int = lib.save_gtfs(
        os.fspath(db_path).encode("utf-8"),
        os.fspath(gtfs_dir_path).encode("utf-8"),
        extern_headers,
        len(headers),
        emit_empty_calendars,
        ensure_order,
    )
    if status:
        raise RuntimeError(f"extern load_gtfs failed with {status}")
