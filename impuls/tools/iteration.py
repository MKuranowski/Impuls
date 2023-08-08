from typing import Iterable, TypeVar

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
