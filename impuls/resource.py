# © Copyright 2022-2025 Mikołaj Kuranowski
# SPDX-License-Identifier: GPL-3.0-or-later

import csv
import json
import logging
import os
from abc import ABC, abstractmethod
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime, parsedate_to_datetime
from io import BytesIO
from pathlib import Path
from tempfile import TemporaryFile
from typing import (
    Any,
    BinaryIO,
    ContextManager,
    Generator,
    Iterator,
    Mapping,
    Optional,
    Sequence,
    TextIO,
    Type,
    Union,
)
from urllib.parse import urlparse
from zipfile import ZipFile, ZipInfo

import requests
import yaml

from .errors import InputNotModified, MultipleDataErrors, ResourceNotCached
from .tools.types import Self, StrPath

FETCH_CHUNK_SIZE = 1024 * 128
"""Preferred size of chunks returned by :py:meth:`~impuls.Resource.fetch`"""

DATETIME_MIN_UTC = datetime.min.replace(tzinfo=timezone.utc)
"""Helper constant with an aware version of datetime.min"""

DATETIME_MAX_UTC = datetime.max.replace(tzinfo=timezone.utc)
"""Helper constant with an aware version of datetime.max"""


logger = logging.getLogger(__name__)

#
# Input resources which can be passed to Pipeline
#


class Resource(ABC):
    """Resource is the base abstract class describing any type of resource,
    which can be downloaded and used by the :py:class:`~impuls.Pipeline`.

    This base class has a abstract method (:py:meth:`fetch`) and 2 abstract, settable
    properties. The latter are implemented by :py:class:`impuls.resource.ConcreteResource`
    (which stores those two properties) and :py:class:`impuls.resource.WrappedResource` (which
    delegates the calls to another Resource). When creating a new type of resource, inherit from
    either of those two classes instead.

    Extra attributes can be preserved across runs by overriding the :py:meth:`save_extra_metadata`
    and :py:meth:`load_extra_metadata` methods.
    """

    @property
    @abstractmethod
    def last_modified(self) -> datetime:
        """last_modified contains the last update time of the resource.
        Only available after a call to fetch. Must be an aware datetime instance.
        """
        ...

    @last_modified.setter
    @abstractmethod
    def last_modified(self, __new: datetime) -> None: ...

    @property
    @abstractmethod
    def fetch_time(self) -> datetime:
        """fetch_time contains the timestamp of the last successful call to fetch.
        Only available after a call to fetch. Must be an aware datetime instance.
        """
        ...

    @fetch_time.setter
    @abstractmethod
    def fetch_time(self, __new: datetime) -> None: ...

    @abstractmethod
    def fetch(self, conditional: bool) -> Iterator[bytes]:
        """fetch returns the content of the resource;
        preferably in chunks of :py:const:`~impuls.resource.FETCH_CHUNK_SIZE` length.

        :py:attr:`~impuls.Resource.last_modified` and :py:attr:`~impuls.Resource.fetch_time`
        attributes of the should be updated right before the first chunk is returned.

        If the conditional is set, the Resource must raise InputNotModified if the resource
        was not modified since :py:attr:`~impuls.Resource.last_modified`. In this case,
        :py:attr:`~impuls.Resource.last_modified` and :py:attr:`~impuls.Resource.fetch_time`
        must not be updated.
        """
        ...

    def save_extra_metadata(self) -> dict[str, Any] | None:
        """Serializes any extra metadata into JSON to be preserved across runs.

        If an empty dictionary or None is returned, extra metadata is not saved.
        """
        return None

    def load_extra_metadata(self, metadata: dict[str, Any]) -> None:
        """Invoked by Impuls resource mechanism to load extra metadata returned by
        :py:meth:`save_extra_metadata`. Not called if a resource has no extra metadata.
        """
        pass


class ConcreteResource(Resource):
    """ConcreteResource is an abstract :py:class:`~impuls.Resource` implementation which stores the
    ``last_modified`` and ``fetch_time`` properties. :py:meth:`~impuls.Resource.fetch`
    still needs to be implemented.

    ``super().__init__()`` must be called by implementing classes in their ``__init__`` methods.
    """

    def __init__(self) -> None:
        self._last_modified: datetime = DATETIME_MIN_UTC
        self._fetch_time: datetime = DATETIME_MIN_UTC

    @property
    def last_modified(self) -> datetime:
        return self._last_modified

    @last_modified.setter
    def last_modified(self, new: datetime) -> None:
        self._last_modified = new

    @property
    def fetch_time(self) -> datetime:
        return self._fetch_time

    @fetch_time.setter
    def fetch_time(self, new: datetime) -> None:
        self._fetch_time = new


class WrappedResource(Resource):
    """WrappedResource is a helper abstract class for implementing modifications to
    existing :py:class:`~impuls.Resource` instances using the
    `decorator pattern <https://en.wikipedia.org/wiki/Decorator_pattern>`_.

    WrappedResource proxies the ``last_modified`` and ``fetch_time`` properties to the
    wrapped resource, but leaves :py:meth:`~impuls.Resource.fetch` unimplemented.

    ``super().__init__()`` must be called by implementing classes in their ``__init__`` methods.
    """

    r: Resource

    def __init__(self, r: Resource) -> None:
        self.r = r

    @property
    def last_modified(self) -> datetime:
        return self.r.last_modified

    @last_modified.setter
    def last_modified(self, new: datetime) -> None:
        self.r.last_modified = new

    @property
    def fetch_time(self) -> datetime:
        return self.r.fetch_time

    @fetch_time.setter
    def fetch_time(self, new: datetime) -> None:
        self.r.fetch_time = new


class LocalResource(ConcreteResource):
    """LocalResource is a :py:class:`~impuls.Resource` located on the local filesystem.

    LocalResources are assumed to be always available, and thus don't need to be cached.

    This however introduces a few issues with the
    last_modified and fetch_times fields when within the :py:class:`~impuls.Pipeline`:

    * :py:attr:`~impuls.Resource.fetch_time` is always the same as
      :py:attr:`~impuls.Resource.last_modified`,
    * :py:attr:`~impuls.Resource.last_modified` is only updated before the pipeline starts

    :py:attr:`~impuls.Resource.fetch_time` thus is not the time when the file was
    last opened, and if the file was modified after :py:class:`~impuls.Pipeline` has started,
    :py:attr:`~impuls.Resource.last_modified` won't be updated;
    but new file content will be returned.

    Those quirks should not be an issue as long as:

    * the file is not modified while the pipeline is running,
    * the pipeline does not rely on the actual access time of the file.
    """

    path: Path

    def __init__(self, path: StrPath) -> None:
        super().__init__()
        self.path = path if isinstance(path, Path) else Path(path)

    def update_last_modified(self, fake_fetch_time: bool = False) -> bool:
        """update_last_modified refreshes the last_modified attribute
        to the modification time of the file; without fetching it.

        If ``fake_fetch_time`` is set to ``True`` (it defaults to ``False``),
        :py:attr:`~impuls.Resource.fetch_time` is also set to the last modification time.
        """
        current_last_modified = datetime.fromtimestamp(self.path.stat().st_mtime, timezone.utc)
        if current_last_modified > self.last_modified:
            self.last_modified = current_last_modified
            if fake_fetch_time:
                self.fetch_time = current_last_modified
            return True
        else:
            return False

    def fetch(self, conditional: bool) -> Iterator[bytes]:
        with open(self.path, mode="rb", buffering=0) as f:
            stat = os.stat(f.fileno())
            current_last_modified = datetime.fromtimestamp(stat.st_mtime, timezone.utc)

            if conditional and current_last_modified <= self.last_modified:
                raise InputNotModified

            self.fetch_time = datetime.now(timezone.utc)
            self.last_modified = current_last_modified

            while chunk := f.read(FETCH_CHUNK_SIZE):
                yield chunk


class HTTPResource(ConcreteResource):
    """HTTPResource is a :py:class:`~impuls.Resource` on a remote server,
    accessible using HTTP or HTTPS.

    Due to limitation of the Last-Modified and If-Modified-Since headers,
    last_modified is precise only to the second, if the file server has updated
    the resource within less than a second, a conditional fetch may not catch such change.
    """

    request: requests.Request
    session: requests.Session
    etag: Optional[str] | None

    def __init__(
        self,
        request: requests.Request,
        session: Optional[requests.Session] = None,
    ) -> None:
        super().__init__()
        self.request = request
        self.session = session or requests.Session()
        self.etag = None

    @classmethod
    def get(
        cls: Type[Self],
        url: str,
        /,
        params: Union[Mapping[str, str], Sequence[tuple[str, str]], None] = None,
        headers: Optional[Mapping[str, str]] = None,
    ) -> Self:
        """get creates a HTTPResource performing a GET request to the provided URL.

        :param params: Optional dictionary or a list of k-v tuples.
          Those parameters are appended to the URL.
        :param headers: Optional dictionary of headers to send to the server.
        """
        return cls(requests.Request("GET", url, params=params, headers=headers))

    @classmethod
    def post(
        cls: Type[Self],
        url: str,
        /,
        params: Union[Mapping[str, str], Sequence[tuple[str, str]], None] = None,
        headers: Optional[Mapping[str, str]] = None,
        data: Union[str, bytes, Mapping[str, str], Sequence[tuple[str, str]], None] = None,
        json: Any = None,
    ) -> Self:
        """post creates a HTTPResource performing a POST request to the provided URL.

        :param params: Optional dictionary or a list of k-v tuples.
          Those parameters are appended to the URL.
        :param headers: Optional dictionary of headers to send to the server.
        :param data: Optional body to attach to the request.
          Apart from a string or bytes, this may be a dictionary or a list of k-v tuples -
          in this case ``data`` is URL-form encoded before sending to the server.
        :param json: Optional the body to attach to the to the request, using JSON encoding.
          If both ``data`` and ``json`` is provided, ``data`` takes precedence.
        """
        return cls(
            requests.Request("POST", url, params=params, headers=headers, data=data, json=json),
        )

    def save_extra_metadata(self) -> dict[str, Any] | None:
        return {"etag": self.etag} if self.etag is not None else None

    def load_extra_metadata(self, metadata: dict[str, Any]) -> None:
        self.etag = metadata.get("etag", None)

    def _do_request(self) -> requests.Response:
        return self.session.send(self.request.prepare(), stream=True)

    def fetch(self, conditional: bool) -> Iterator[bytes]:
        # Set the conditional request headers
        if conditional and self.etag is not None:
            self.request.headers.pop("If-Modified-Since", None)
            self.request.headers["If-None-Match"] = self.etag
        elif conditional and self.last_modified != DATETIME_MIN_UTC:
            self.request.headers.pop("If-None-Match", None)
            self.request.headers["If-Modified-Since"] = format_datetime(
                self.last_modified.astimezone(timezone.utc),
                usegmt=True,
            )
        else:
            self.request.headers.pop("If-Modified-Since", None)
            self.request.headers.pop("If-None-Match", None)

        # Perform the request
        with self._do_request() as resp:
            # Stop conditional requests
            if resp.status_code == 304:
                assert conditional, "304 response are only possible with conditional requests"
                raise InputNotModified

            resp.raise_for_status()

            self.fetch_time = datetime.now(timezone.utc)
            self.etag = resp.headers.get("ETag")
            if last_modified_str := resp.headers.get("Last-Modified"):
                self.last_modified = parsedate_to_datetime(last_modified_str)
                assert self.last_modified.tzinfo is timezone.utc
            else:
                logger.error("%s did not send the Last-Modified header", urlparse(resp.url).netloc)

            for chunk in resp.iter_content(FETCH_CHUNK_SIZE, decode_unicode=False):
                yield chunk


class TimeLimitedResource(WrappedResource):
    """TimeLimitedResource wraps a :py:class:`~impuls.Resource` and ensures
    the time between conditional fetches is at least ``minimal_time_between``.

    TimeLimitedResource can be used to cache constantly-changing resources
    or to prevent bothering an external server.
    """

    minimal_time_between: timedelta
    """minimal time which must pass between fetches to the external server"""

    def __init__(self, r: Resource, minimal_time_between: timedelta) -> None:
        super().__init__(r)
        self.minimal_time_between = minimal_time_between

    def fetch(self, conditional: bool) -> Iterator[bytes]:
        # Stop time-limited requests
        now = datetime.now(timezone.utc)
        since_last_fetch = now - self.fetch_time
        if conditional and since_last_fetch < self.minimal_time_between:
            raise InputNotModified

        # Delegate the call to the wrapped resource
        return self.r.fetch(conditional)


class ZippedResource(WrappedResource):
    """ZippedResource wraps a :py:class:`~impuls.Resource` pointing to a zip archive,
    creating a Resource which reads the content of one file from that archive.

    - :py:attr:`.file_name_in_zip` dictates which file to extract from the archive.
        It defaults to None, which first checks if there's one file in the archive,
        and extracts it.
    - :py:attr:`.save_zip_in_memory` dictates whether the zipfile can be saved in memory.
        It defaults to ``True``, but if the archive itself is huge this option may be
        set to ``False``, causing the zip file to be written to a temporary file.
    """

    file_name_in_zip: str | None
    save_zip_in_memory: bool

    def __init__(
        self,
        r: Resource,
        file_name_in_zip: str | None = None,
        save_zip_in_memory: bool = True,
    ) -> None:
        super().__init__(r)
        self.file_name_in_zip = file_name_in_zip
        self.save_zip_in_memory = save_zip_in_memory

    def fetch(self, conditional: bool) -> Iterator[bytes]:
        with self.fetch_zip(conditional) as zip_buffer, ZipFile(zip_buffer) as zip:
            with zip.open(self.pick_file(zip), mode="r") as buffer:
                while chunk := buffer.read(FETCH_CHUNK_SIZE):
                    yield chunk

    def pick_file(self, in_: ZipFile) -> ZipInfo:
        """Picks the file to decompress."""
        if self.file_name_in_zip is None:
            files = in_.infolist()
            if len(files) != 1:
                raise ValueError(f"Expected one file in ZIP, got {len(files)}")
            return files[0]

        try:
            return in_.getinfo(self.file_name_in_zip)
        except KeyError as e:
            raise ValueError(f"Can't find file {self.file_name_in_zip!r} in ZIP") from e

    def fetch_zip(self, conditional: bool) -> ContextManager[BinaryIO]:
        """Fetches the bytes of the zip file, depending on the :py:attr:`.save_zip_in_memory`
        setting."""
        if self.save_zip_in_memory:
            return self.fetch_zip_to_memory(conditional)
        else:
            return self.fetch_zip_to_temp_file(conditional)

    def fetch_zip_to_memory(self, conditional: bool) -> BytesIO:
        """Fetches the zipfile to a BytesIO and returns it."""
        b = BytesIO()
        for chunk in self.r.fetch(conditional):
            b.write(chunk)
        return b

    @contextmanager
    def fetch_zip_to_temp_file(self, conditional: bool) -> Generator[BinaryIO, None, None]:
        """Fetches the zipfile to a TemporaryFile and returns it"""
        with TemporaryFile(mode="w+b", prefix="impuls-zip") as temp_file:
            for chunk in self.r.fetch(conditional):
                temp_file.write(chunk)
            temp_file.seek(0)
            yield temp_file


#
# Output resource, passed to Pipeline tasks.
#


@dataclass(frozen=True)
class ManagedResource:
    """ManagedResource represents a resource which has been cached† by a
    :py:class:`~impuls.Pipeline`.

    The name may be confusing, it does not implement the :py:class:`~impuls.Resource`
    protocol; it's not an input to the Pipeline, rather a ManagedResource is the output of
    :py:class:`~impuls.Pipeline`.

    ManagedResources should not be modified. However, if they are modified:

    * all tasks following the modifying task will receive the modified ManagedResource.
    * all tasks preceding and including the modifying task may receive a modified
        ManagedResource or an unmodified fresh copy of the Resource.

    † - LocalResources are not cached; in this case :py:attr:`.stored_at` is the same as the
    original :py:attr:`LocalResource.path`.
    """

    stored_at: Path
    """stored_at is the Path to the cached resource"""

    last_modified: datetime = DATETIME_MIN_UTC
    """last_modified is the last_modified time of the original Resource.

    Note that this is different than stat().st_mtime,
    which is unrelated to the original Resource.
    """

    fetch_time: datetime = DATETIME_MIN_UTC
    """fetch_time is the time when the original Resource was downloaded.

    Unavailable if the original resource was a LocalResource (will be the same as last_modified).
    """

    def stat(self) -> os.stat_result:
        """stat returns the stat result of the cached file with Resource content"""
        return self.stored_at.stat()

    def size(self) -> int:
        """size returns the size of the file in bytes"""
        return self.stored_at.stat().st_size

    def open_text(
        self,
        buffering: int = -1,
        encoding: Optional[str] = None,
        errors: Optional[str] = None,
        newline: Optional[str] = None,
    ) -> TextIO:
        """open_text opens the cached file in "r" mode, with the provided arguments"""
        return self.stored_at.open(
            mode="r",
            buffering=buffering,
            encoding=encoding,
            errors=errors,
            newline=newline,
        )

    def open_binary(self, buffering: int = -1) -> BinaryIO:
        """open_text opens the cached file in "rb" mode, with the provided arguments"""
        return self.stored_at.open(mode="rb", buffering=buffering)

    def text(self, encoding: Optional[str] = None, errors: Optional[str] = None) -> str:
        """text reads the content of the file into a string.
        If encoding and errors are not defined, system settings are used.
        """
        return self.stored_at.read_text(encoding, errors)

    def bytes(self) -> bytes:
        """bytes reads the content of the file into a bytes object."""
        return self.stored_at.read_bytes()

    def json(
        self,
        encoding: Optional[str] = None,
        errors: Optional[str] = None,
        **json_load_kwargs: Any,
    ) -> Any:
        """json deserializes resource content using JSON.

        File is opened in "r" mode, and if encoding and errors are not defined,
        system settings are used.

        Any other keyword arguments are passed to json.load.
        """
        with self.stored_at.open(mode="r", encoding=encoding, errors=errors) as f:
            return json.load(f, **json_load_kwargs)

    def yaml(self, encoding: Optional[str] = None, errors: Optional[str] = None) -> Any:
        """yaml deserializes resource content using YAML, using yaml.safe_load.

        File is opened in "r" mode, and if encoding and errors are not defined,
        system settings are used.
        """
        with self.stored_at.open(mode="r", encoding=encoding, errors=errors) as f:
            return yaml.safe_load(f)

    def csv(
        self,
        encoding: Optional[str] = None,
        errors: Optional[str] = None,
        **csv_dict_reader_kwargs: Any,
    ) -> Iterator[dict[str, str]]:
        """csv reads CSV records from the resource.

        File is opened in "r" mode, and if encoding and errors are not defined,
        system settings are used.

        Any other keyword arguments are passed to csv.DictReader constructor.
        """
        with self.stored_at.open(mode="r", newline="", encoding=encoding, errors=errors) as f:
            yield from csv.DictReader(f, **csv_dict_reader_kwargs)


#
# Routines to cache input resources into ManagedResource
#


def _cache_path_of_resource(r: Resource, name: str, workspace: Path) -> Path:
    """_cache_path_of_resource returns the location where a resource should be cached,
    unless resource is a LocalResource - in this case returns that local resource path.

    >>> _cache_path_of_resource(
    ...     HTTPResource.get("https://example.com"),
    ...     "example.html",
    ...     Path("workspace"),
    ... ).parts
    ('workspace', 'example.html')
    >>> _cache_path_of_resource(
    ...     LocalResource("foo/bar.json"),
    ...     "list_of_lines.json",
    ...     Path("workspace"),
    ... ).parts
    ('foo', 'bar.json')
    """
    if isinstance(r, LocalResource):
        return r.path
    return workspace / name


def _read_metadata(r: Resource, metadata_path: Path) -> None:
    """_read_metadata_to updates resource last_modified and fetch_time
    based on data from the metadata_path.

    If the metadata file does not exist, DATETIME_MIN_UTC is set for both attributes."""
    try:
        with metadata_path.open("r", encoding="ascii") as f:
            metadata = json.load(f)
            r.last_modified = datetime.fromtimestamp(metadata["last_modified"], timezone.utc)
            r.fetch_time = datetime.fromtimestamp(metadata["fetch_time"], timezone.utc)
            if extra_metadata := metadata.get("extra"):
                r.load_extra_metadata(extra_metadata)
    except FileNotFoundError:
        r.last_modified = DATETIME_MIN_UTC
        r.fetch_time = DATETIME_MIN_UTC


def _write_metadata(r: Resource, metadata_path: Path) -> None:
    """_write_metadata_of saves resource last_modified and fetch_time attributes
    into metadata_path, for it to be later read with _read_metadata."""
    metadata: dict[str, Any] = {
        "last_modified": r.last_modified.timestamp(),
        "fetch_time": r.fetch_time.timestamp(),
    }

    if extra_metadata := r.save_extra_metadata():
        metadata["extra"] = extra_metadata

    with metadata_path.open("w", encoding="ascii") as f:
        json.dump(metadata, f)


def _download_resource(r: Resource, to: Path, conditional: bool = True) -> None:
    """_cache_resource ensures the latest content of the resource
    is saved in the ``to`` file.
    """
    # Dump the contents to a temporary file, and swap it with the target file
    # only on successful fetch.
    #
    # If `to` was opened with "wb", it will be truncated;
    # which is not correct if fetch raises InputNotModified -
    # the content will be lost.
    #
    # Sure, this could be fixed by only truncating the file
    # when the first chunk is returned; but the content
    # of the resource will be lost if the fetch() fails partway through.
    #
    # Going through a temporary file allows the user to run the pipeline
    # from cache if fetch() fails partway through.

    temp_to = to.parent / (to.name + ".tmp")
    try:
        with temp_to.open(mode="wb", buffering=0) as f:
            for chunk in r.fetch(conditional):
                f.write(chunk)
        temp_to.rename(to)
    finally:
        temp_to.unlink(missing_ok=True)


def cache_resources(
    r: Mapping[str, Resource],
    workspace: Path,
) -> tuple[dict[str, ManagedResource], bool]:
    """cache_resources ensures all resources are stored locally by fetching outdated resources.

    First returned element is a mapping from resource name to its :py:class:`ManagedResource`
    counterpart.

    Second returned element is a flag set to ``True`` if at least one :py:class:`~impuls.Resource`
    was fetched. It's set to ``False`` if there are no resources.
    """
    modified: bool = False
    managed_resources: dict[str, ManagedResource] = {}

    for name, res in r.items():
        logger.info("Refreshing %s (downloading if it has changed)", name)

        # TODO: Check if the resource name can be used as a filename
        cached_path = _cache_path_of_resource(res, name, workspace)
        metadata_path = workspace / (name + ".metadata")
        _read_metadata(res, metadata_path)

        if isinstance(res, LocalResource):
            this_was_modified = res.update_last_modified(fake_fetch_time=True)
        else:
            this_was_modified = True
            try:
                _download_resource(res, cached_path)
            except InputNotModified:
                this_was_modified = False

        logger.debug("%s was %s", name, "modified" if this_was_modified else "not modified")

        _write_metadata(res, metadata_path)
        managed_resources[name] = ManagedResource(cached_path, res.last_modified, res.fetch_time)
        modified = modified or this_was_modified

    return managed_resources, modified


def _ensure_resource_cached(
    r: Resource,
    name: str,
    workspace: Path,
) -> tuple[str, ManagedResource]:
    """_ensure_resource_cached checks that a resource is stored at its cache path,
    restoring the resource metadata. If a :py:class:`~impuls.Resource` is not cached raises
    :py:exc:`~impuls.errors.ResourceNotCached`.

    Returns the name of the resource alongside a :py:class:`ManagedResource`.
    """
    cached_path = _cache_path_of_resource(r, name, workspace)
    if not cached_path.exists():
        raise ResourceNotCached(name)

    metadata_path = workspace / (name + ".metadata")
    if isinstance(r, LocalResource):
        r.update_last_modified(fake_fetch_time=True)
    else:
        _read_metadata(r, metadata_path)
    return name, ManagedResource(cached_path, r.last_modified, r.fetch_time)


def ensure_resources_cached(
    r: Mapping[str, Resource],
    workspace: Path,
) -> dict[str, ManagedResource]:
    """ensure_resources_cached ensures all resources are stored locally
    **without** fetching any resources. If any resource is not cached, raises
    :py:exc:`~impuls.errors.MultipleDataErrors` with a list of
    :py:exc:`~impuls.errors.ResourceNotCached` corresponding to all missing resources.

    Never raises :py:exc:`~impuls.errors.InputNotModified`.

    Returns a mapping from resource name to its :py:class:`ManagedResource` counterpart.
    """

    # Function to be passed to MultipleDataErrors.catch_all
    def ensure_cached(arg: tuple[str, Resource]) -> tuple[str, ManagedResource]:
        name, res = arg
        return _ensure_resource_cached(res, name, workspace)

    logger.info("Checking resources")

    managed_resources = dict(
        MultipleDataErrors.catch_all(
            "ensure_resources_cached",
            map(ensure_cached, r.items()),
        )
    )

    return managed_resources


def prepare_resources(
    r: Mapping[str, Resource],
    workspace: Path,
    from_cache: bool = False,
) -> tuple[dict[str, ManagedResource], bool]:
    """prepare_resources ensures all provided :py:class:`~impuls.Resource` instances are available
    locally.

    If ``from_cache`` is False, missing or stale resources are fetched; otherwise
    :py:exc:`~impuls.errors.MultipleDataErrors` with :py:exc:`~impuls.errors.ResourceNotCached`
    may be raised.

    First returned element is a mapping from resource name
    to its :py:class:`ManagedResource` counterparts.

    The second returned flag, indicating whether the :py:class:`~impuls.Pipeline` should continue.
    It's set to ``True`` if there are no resources, ``from_cache`` is ``True`` or at least
    one :py:class:`~impuls.Resource` was cached.
    """
    if not r:
        return {}, True
    elif from_cache:
        # Asked not to download any resources - just ensure they're all cached
        return ensure_resources_cached(r, workspace), True
    else:
        return cache_resources(r, workspace)
