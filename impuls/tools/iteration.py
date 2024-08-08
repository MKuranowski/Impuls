# © Copyright 2022-2024 Mikołaj Kuranowski
# SPDX-License-Identifier: GPL-3.0-or-later

from collections.abc import Sized
from typing import Any, Iterable, TypeVar

T = TypeVar("T")


def limit(it: Iterable[T], n: int) -> Iterable[T]:
    """limit limits the iterator to at most n elements.

    >>> list(limit([1, 2, 3, 4, 5], 3))
    [1, 2, 3]
    >>> from itertools import repeat
    >>> list(limit(repeat(5), 5))
    [5, 5, 5, 5, 5]
    """
    for i, elem in enumerate(it):
        if i == n:
            break
        yield elem


def walk_len(it: Iterable[Any]) -> int:
    """Checks how many elements are in the iterable.
    Exhausts the iterable, unless `len(...)` works on it.

    >>> walk_len(i + 1 for i in range(5))
    5
    >>> walk_len(i for i in range(5) if i % 2 == 0)
    3
    """
    if isinstance(it, Sized):
        return len(it)
    else:
        return sum(1 for _ in it)
