# © Copyright 2022-2024 Mikołaj Kuranowski
# SPDX-License-Identifier: GPL-3.0-or-later

from os import PathLike
from typing import TYPE_CHECKING, TypeGuard, TypeVar

if TYPE_CHECKING:
    from typing_extensions import Self
else:
    Self = TypeVar("Self")


SQLNativeType = None | int | float | str
"""SQLNativeType is a Union of types which can be directly returned by the SQLite engine:
``None``, ``int``, ``float`` and ``str``.
"""

StrPath = str | PathLike[str]
"""StrPath represents anything which can be interpreted as a string-based path."""

BytesPath = bytes | PathLike[bytes]
"""StrPath represents anything which can be interpreted as a bytes-based path."""

AnyPath = str | bytes | PathLike[str] | PathLike[bytes]
"""AnyPath represents anything which can be interpreted as a path."""

T = TypeVar("T")


def identity(x: T) -> T:
    """identity returns ``x`` unchanged.

    >>> identity(42)
    42
    """
    return x


def all_non_none(lst: list[T | None]) -> TypeGuard[list[T]]:
    """Returns ``True`` if all elements of ``lst`` are not ``None``.

    >>> all_non_none([0, 1, 2, 3, 4])
    True
    >>> all_non_none(["", "a", "b", None, "c"])
    False
    """
    return all(i is not None for i in lst)
