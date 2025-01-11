# © Copyright 2022-2024 Mikołaj Kuranowski
# SPDX-License-Identifier: GPL-3.0-or-later

import re
from itertools import count
from typing import Container

ILLEGAL_PORTABLE_NAMES: frozenset[str] = frozenset(
    (
        ".",
        "..",
        "CON",
        "PRN",
        "AUX",
        "CLOCK$",
        "NUL",
        "COM1",
        "COM2",
        "COM3",
        "COM4",
        "COM5",
        "COM6",
        "COM7",
        "COM8",
        "COM9",
        "LPT1",
        "LPT2",
        "LPT3",
        "LPT4",
        "LPT5",
        "LPT6",
        "LPT7",
        "LPT8",
        "LPT9",
    )
)
"""Set of filenames which can't be reliably used on some operating systems.

For example, "." and ".." (dot and dot-dot) have
`special meaning in POSIX <https://pubs.opengroup.org/onlinepubs/9699919799/basedefs/V1_chap03.html#tag_03_170>`_,
while `Windows has its own set of forbidden filenames <http://web.archive.org/web/20120414111738/http://www.blindedbytech.com/2006/11/16/forbidden-file-and-folder-names-on-windows>`_.
"""  # noqa: E501


def camel_to_snake(camel: str) -> str:
    """Converts camelCase or PascalCase to snake_case.

    >>> camel_to_snake("Foo")
    'foo'
    >>> camel_to_snake("FooBar")
    'foo_bar'
    >>> camel_to_snake("fooBarBaz")
    'foo_bar_baz'
    """
    return re.sub(r"\B[A-Z]", lambda m: f"_{m[0]}", camel).lower()


def is_portable_name(name: str) -> bool:
    """Checks if a name can be used as a filename - that is it only contains
    ASCII letters, digits, dot, hyphen or underscores, contains at least one character,
    and has no special meaning on certain systems (like ".", ".." or "COM")
    """
    # Allowed characters come from POSIX:
    # https://pubs.opengroup.org/onlinepubs/9699919799/basedefs/V1_chap03.html#tag_03_282
    # Note that some systems treat filenames as case-insensitive.
    return (
        name.upper() not in ILLEGAL_PORTABLE_NAMES
        and re.match(r"^[A-Za-z0-9._-]+$", name, re.ASCII) is not None
    )


def find_non_conflicting_id(used: Container[str], id: str, separator: str = ":") -> str:
    """Tries to find the lowest numeric suffix (joined with separator) to the id
    which generates a string not contained in `used`.

    >>> find_non_conflicting_id({"A", "B"}, "C")
    'C'
    >>> find_non_conflicting_id({"A", "B"}, "A")
    'A:1'
    >>> find_non_conflicting_id({"A", "A/1", "A/2"}, "A", separator="/")
    'A/3'
    """
    if id not in used:
        return id

    for suffix in count(1):
        candidate = f"{id}{separator}{suffix}"
        if candidate not in used:
            return candidate

    raise RuntimeError("not reachable - itertools.count must be infinite")
