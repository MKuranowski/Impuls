# © Copyright 2022-2024 Mikołaj Kuranowski
# SPDX-License-Identifier: GPL-3.0-or-later

import itertools
import operator
import os
import unittest.mock
from contextlib import ExitStack, contextmanager
from datetime import datetime, timedelta, timezone, tzinfo
from pathlib import Path
from shutil import rmtree
from tempfile import mkdtemp, mkstemp
from typing import Any, Generator, Iterable, Iterator, Mapping, Optional, Protocol, Type

import requests

from ..errors import InputNotModified
from ..resource import DATETIME_MIN_UTC, ConcreteResource
from .types import Self


class DatetimeNowLike(Protocol):
    def __call__(self, tz: Optional[tzinfo] = ...) -> datetime: ...


class MockDatetimeNow:
    """
    MockDatetimeNow is a helper for mocking datetime.now,
    by returning datetimes from a provided iterator.

    Once the provided iterator runs out, StopIteration is raised.

    >>> fake_dt_now = MockDatetimeNow([
    ...     datetime(2020, 1, 30, 5, 10),
    ...     datetime(2020, 1, 30, 5, 20),
    ...     datetime(2020, 1, 30, 5, 30),
    ...     datetime(2020, 1, 30, 5, 40),
    ... ]).now
    >>> fake_dt_now()
    datetime.datetime(2020, 1, 30, 5, 10)
    >>> fake_dt_now()
    datetime.datetime(2020, 1, 30, 5, 20)
    >>> fake_dt_now()
    datetime.datetime(2020, 1, 30, 5, 30)
    >>> fake_dt_now()
    datetime.datetime(2020, 1, 30, 5, 40)
    >>> fake_dt_now()
    Traceback (most recent call last):
        ...
    StopIteration
    """

    def __init__(self, times: Iterable[datetime]) -> None:
        self.it = iter(times)

    def now(self, tz: Optional[tzinfo] = None) -> datetime:
        dt = next(self.it)
        return dt.astimezone(tz) if tz else dt

    @classmethod
    def constant(cls: Type[Self], t: datetime) -> Self:
        return cls(itertools.repeat(t))

    @classmethod
    def evenly_spaced(cls: Type[Self], start: datetime, delta: timedelta) -> Self:
        """evenly_spaced provides an infinite MockDatetimeNow
        which returns (start, start + delta, start + 2*delta, ...).

        >>> fake_dt_now = (
        ...     MockDatetimeNow
        ...     .evenly_spaced(datetime(2020, 1, 30, 5, 10), timedelta(minutes=10))
        ...     .now
        ... )
        >>> fake_dt_now()
        datetime.datetime(2020, 1, 30, 5, 10)
        >>> fake_dt_now()
        datetime.datetime(2020, 1, 30, 5, 20)
        >>> fake_dt_now()
        datetime.datetime(2020, 1, 30, 5, 30)
        """
        return cls(itertools.accumulate(itertools.repeat(delta), operator.add, initial=start))

    @contextmanager
    def patch(self, *datetime_targets: str) -> Generator[None, None, None]:
        with ExitStack() as s:
            for datetime_target in datetime_targets:
                mock = s.enter_context(unittest.mock.patch(datetime_target, wraps=datetime))
                mock.now.side_effect = self.now  # type: ignore
            yield


class MockHTTPResponse:
    """MockHTTPResponse tries to mimic the requests.Response object.
    Only methods and attributes required for the tests are implemented.

    >>> r = MockHTTPResponse(200, b"Hello!")
    >>> r.status_code
    200
    >>> r.content
    b'Hello!'
    >>> r.headers
    {}
    """

    def __init__(
        self,
        status_code: int,
        content: bytes = b"",
        headers: Optional[Mapping[str, str]] = {},
    ) -> None:
        self.status_code = status_code
        self.content = content
        self.headers = headers or {}
        self.url = ""

    def __enter__(self) -> "MockHTTPResponse":
        """
        The context manager for MockHTTPResponse does nothing.
        >>> with MockHTTPResponse(200, b"Hello!") as r:
        ...     r.status_code, r.content
        (200, b'Hello!')
        """
        return self

    def __exit__(self, *_: Any) -> bool:
        return False

    def raise_for_status(self) -> None:
        """Raises requests.HTTPError if the status_code is bigger than or equal to 400.

        >>> MockHTTPResponse(200).raise_for_status()
        >>> MockHTTPResponse(404).raise_for_status()
        Traceback (most recent call last):
            ...
        requests.exceptions.HTTPError: 404
        """
        if self.status_code >= 400:
            raise requests.HTTPError(str(self.status_code))

    def iter_content(self, chunk_size: int = 16, decode_unicode: bool = False) -> Iterable[bytes]:
        """iter_content generates self.content in chunks of the provided size.
        For now decode_unicode must be False.

        >>> r = MockHTTPResponse(200, b"Lorem ipsum dolor sit")
        >>> list(r.iter_content(8))
        [b'Lorem ip', b'sum dolo', b'r sit']
        """
        assert not decode_unicode

        rest = self.content
        while rest:
            yield rest[:chunk_size]
            rest = rest[chunk_size:]


class MockResource(ConcreteResource):
    """MockResource mocks a Resource, returning a predefined content
    and allowing control over the last_modified attribute.
    """

    content: bytes
    clock: DatetimeNowLike
    persistent_last_modified: datetime | None
    extra_metadata: dict[str, Any] | None

    def __init__(
        self,
        content: bytes = b"",
        fetch_time: datetime = DATETIME_MIN_UTC,
        last_modified: datetime = DATETIME_MIN_UTC,
        clock: DatetimeNowLike = datetime.now,
        persist_last_modified: bool = False,
        extra_metadata: dict[str, Any] | None = None,
    ) -> None:
        super().__init__()
        self.last_modified = last_modified
        self.fetch_time = fetch_time

        self.content = content
        self.clock = clock
        self.persistent_last_modified = fetch_time if persist_last_modified else None
        self.extra_metadata = extra_metadata

    def fetch(self, conditional: bool) -> Iterator[bytes]:
        # cache_resources overwrites last_modified of the Resource.
        # in many tests, we want to substitute our own, newer last_modified.
        if self.persistent_last_modified is not None:
            self.last_modified = self.persistent_last_modified

        if (
            conditional
            and self.last_modified > DATETIME_MIN_UTC
            and self.last_modified <= self.fetch_time
        ):
            raise InputNotModified
        self.fetch_time = self.clock(timezone.utc)
        yield self.content

    def refresh(self) -> None:
        self.last_modified = self.clock(timezone.utc)

    def save_extra_metadata(self) -> dict[str, Any] | None:
        return self.extra_metadata

    def load_extra_metadata(self, metadata: dict[str, Any]) -> None:
        self.extra_metadata = metadata


class MockFile:
    """MockFile creates a temporary file for testing purposes.
    The file must be removed after usage by calling mock_file.cleanup().
    This action is automatically performed if MockFile is used in a with statement.

    >>> with MockFile() as f:
    ...     _ = f.write_text("Hello, world!")
    ...     f.read_text()
    'Hello, world!'
    """

    path: Path

    def __init__(
        self, prefix: str = "impuls-test", suffix: Optional[str] = None, directory: bool = False
    ) -> None:
        if directory:
            path = mkdtemp(prefix=prefix, suffix=suffix)
        else:
            handle, path = mkstemp(prefix=prefix, suffix=suffix)
            os.close(handle)
        self.path = Path(path)

    def __enter__(self) -> Path:
        return self.path

    def __exit__(self, *_: Any) -> bool:
        self.cleanup()
        return False

    def cleanup(self) -> None:
        if self.path.is_dir():
            rmtree(self.path)
        elif self.path.exists():
            self.path.unlink()
