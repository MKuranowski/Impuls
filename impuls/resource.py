import csv
import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime, parsedate_to_datetime
from pathlib import Path
from typing import (
    Any,
    BinaryIO,
    Iterator,
    Mapping,
    Optional,
    Protocol,
    Sequence,
    TextIO,
    Type,
    Union,
)
from urllib.parse import urlparse

import requests
import yaml

from .errors import InputNotModified, MultipleDataErrors, ResourceNotCached
from .tools.types import Self

FETCH_CHUNK_SIZE = 1024 * 128
DATETIME_MIN_UTC = datetime.min.replace(tzinfo=timezone.utc)
DATETIME_MAX_UTC = datetime.max.replace(tzinfo=timezone.utc)


logger = logging.getLogger(__name__)

#
# Input resources which can be passed to Pipeline
#


class Resource(Protocol):
    """Resource is a protocol describing any type of resources,
    which can be downloaded and used by the pipeline.
    """

    last_modified: datetime
    """last_modified contains the last update time of the resource.
    Only available after a call to fetch. Must be an aware datetime instance.
    """

    fetch_time: datetime
    """fetch_time contains the timestamp of the last successful call to fetch.
    Only available after a call to fetch. Must be an aware datetime instance.
    """

    def fetch(self, conditional: bool) -> Iterator[bytes]:
        """fetch returns the content of the resource;
        preferably in chunks of `FETCH_CHUNK_SIZE` length.

        `last_modified` and `fetch_time` attributes of the should
        be updated right before the first chunk is returned.

        If conditional is set to true, the Resource must raise
        InputNotModified if the resource was not modified since
        `last_modified`. In this case, `last_modified` and
        `fetch_time` must not be updated.
        """
        ...


class LocalResource(Resource):
    """LocalResource is a Resource located on the local filesystem.

    LocalResources are assumed to be always available, and thus need not be cached.

    This however introduces a few issues with the
    last_modified and fetch_times fields when within the Pipeline:
    - fetch_time is always the same as last_modified,
    - last_modified is only updated before the pipeline starts

    fetch_time thus is not the time when the file was last opened,
    and if the file was modified after Pipeline has started, last_modified won't be updated;
    but new file content will be returned.

    Those quirks should not be an issue as long as:
    - the file is not modified while the pipeline is running,
    - the pipeline does not rely on fetch_time.
    """

    path: Path
    last_modified: datetime
    fetch_time: datetime

    def __init__(self, path: Union[str, Path]) -> None:
        self.path = path if isinstance(path, Path) else Path(path)
        self.last_modified = DATETIME_MIN_UTC
        self.fetch_time = DATETIME_MIN_UTC

    def update_last_modified(self, fake_fetch_time: bool = False) -> bool:
        """update_last_modified refreshes the last_modified attribute
        to the modification time of the file; without fetching it.

        If fake_fetch_time is set to true (it defaults to False),
        fetch_time is also set to the last modification time.
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


class HTTPResource(Resource):
    """HTTPResource is a Resource on a remote server, accessible using HTTP or HTTPS.

    Due to limitation of the Last-Modified and If-Modified-Since headers,
    last_modified is precise only to the second, if the file server has updated
    the resource within less than a second, a conditional fetch may not catch such change.
    """

    request: requests.Request
    session: requests.Session
    last_modified: datetime

    def __init__(
        self,
        request: requests.Request,
        session: Optional[requests.Session] = None,
    ) -> None:
        self.request = request
        self.session = session or requests.Session()
        self.fetch_time = DATETIME_MIN_UTC
        self.last_modified = DATETIME_MIN_UTC

    @classmethod
    def get(
        cls: Type[Self],
        url: str,
        /,
        params: Union[Mapping[str, str], Sequence[tuple[str, str]], None] = None,
        headers: Optional[Mapping[str, str]] = None,
    ) -> Self:
        """get creates a HTTPResource performing a GET request to the provided URL.

        params, if provided, should be a dictionary or a list of k-v tuples.
        Those parameters are appended to the URL.

        headers, if provided, should be a dictionary of headers to send to the server.
        """
        return cls(requests.Request("GET", url, params=params, headers=headers))

    @classmethod
    def post(
        cls: Type[Self],
        url: str,
        /,
        params: Union[Mapping[str, str], Sequence[tuple[str, str]], None] = None,
        headers: Optional[Mapping[str, str]] = None,
        data: Union[Mapping[str, str], Sequence[tuple[str, str]], None] = None,
        json: Any = None,
    ) -> Self:
        """post creates a HTTPResource performing a POST request to the provided URL.

        params, if provided, are should be a dictionary or a list of k-v tuples.
        Those parameters are appended to the URL.

        headers, if provided, should be a dictionary of headers to send to the server.

        data, if provided, is the body to attach to the request.
        May be a dictionary or a list of k-v tuples - in this case data is URL-form encoded
        before sending to the server.

        json, if provided, is the body to attach to the to the request, using JSON encoding.
        If both data and json is provided, data takes precedence.
        """
        return cls(
            requests.Request("POST", url, params=params, headers=headers, data=data, json=json),
        )

    def _do_request(self) -> requests.Response:
        return self.session.send(self.request.prepare(), stream=True)

    def fetch(self, conditional: bool) -> Iterator[bytes]:
        # Set the conditional request headers
        if conditional and self.last_modified != DATETIME_MIN_UTC:
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
                raise InputNotModified

            resp.raise_for_status()

            self.fetch_time = datetime.now(timezone.utc)
            if last_modified_str := resp.headers.get("Last-Modified"):
                self.last_modified = parsedate_to_datetime(last_modified_str)
                assert self.last_modified.tzinfo is timezone.utc
            else:
                logger.error("%s did not send the Last-Modified header", urlparse(resp.url).netloc)

            for chunk in resp.iter_content(FETCH_CHUNK_SIZE, decode_unicode=False):
                yield chunk


class TimeLimitedResource(Resource):
    """TimeLimitedResource wraps an Resource and ensures the time between conditional
    fetches is at least `minimal_time_between`.

    TimeLimitedResource can be used to cache constantly-changing resources
    or to prevent bothering an external server.
    """

    r: Resource
    minimal_time_between: timedelta

    def __init__(self, r: Resource, minimal_time_between: timedelta) -> None:
        self.r = r
        self.minimal_time_between = minimal_time_between

    @property
    def last_modified(self) -> datetime:
        return self.r.last_modified

    @last_modified.setter
    def last_modified(self, new: datetime) -> None:  # type: ignore
        self.r.last_modified = new

    @property
    def fetch_time(self) -> datetime:
        return self.r.fetch_time

    @fetch_time.setter
    def fetch_time(self, new: datetime) -> None:  # type: ignore
        self.r.fetch_time = new

    def fetch(self, conditional: bool) -> Iterator[bytes]:
        # Stop time-limited requests
        now = datetime.now(timezone.utc)
        since_last_fetch = now - self.fetch_time
        if conditional and since_last_fetch < self.minimal_time_between:
            raise InputNotModified

        # Delegate the call to the wrapped resource
        return self.r.fetch(conditional)


#
# Output resource, passed to Pipeline tasks.
#


@dataclass(frozen=True)
class ManagedResource:
    """ManagedResource represents a resource which has been cached† by a Pipeline.

    The name may be confusing, it does not implement the Resource protocol;
    it's not an input to the Pipeline, rather a ManagedResource is the output of Pipeline.

    ManagedResources should not be modified. However, if they are modified:
    - all tasks following the modifying task will receive the modified ManagedResource.
    - all tasks preceding and including the modifying task
      may receive a modified ManagedResource or an unmodified fresh copy of the Resource.

    † - LocalResources are not cached; in this case stored_at
    is the same as the original Resource path.
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

    def open_text(self, **open_args: Any) -> TextIO:
        """open_text opens the cached file in "r" mode, with the provided arguments"""
        return self.stored_at.open(mode="r", **open_args)

    def open_binary(self, **open_args: Any) -> BinaryIO:
        """open_text opens the cached file in "rb" mode, with the provided arguments"""
        return self.stored_at.open(mode="rb", **open_args)  # type: ignore

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
    except FileNotFoundError:
        r.last_modified = DATETIME_MIN_UTC
        r.fetch_time = DATETIME_MIN_UTC


def _write_metadata(r: Resource, metadata_path: Path) -> None:
    """_write_metadata_of saves resource last_modified and fetch_time attributes
    into metadata_path, for it to be later read with _read_metadata."""
    with metadata_path.open("w", encoding="ascii") as f:
        json.dump(
            {
                "last_modified": r.last_modified.timestamp(),
                "fetch_time": r.fetch_time.timestamp(),
            },
            f,
        )


def _download_resource(r: Resource, to: Path, conditional: bool = True) -> None:
    """_cache_resource ensures the latest content of the resource
    is saved in the `to` file.
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

    First returned element is a mapping from resource name
    to its ManagedResource counterpart.

    Second returned element is a flag se to True if at least one Resource
    was fetched. It's set to False if there are no Resources.
    """
    modified: bool = False
    managed_resources: dict[str, ManagedResource] = {}

    for name, res in r.items():
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
    restoring the resource metadata. If a resource is not cached raises ResourceNotCached.
    Returns the name of the resource alongside a ManagedResource.
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
    **without** fetching any resources. If any resource is not cached, raises MultipleDataErrors
    with a list of ResourceNotCached corresponding to all missing resources.

    Never raises InputNotModified.

    Returns a mapping from resource name to its ManagedResource counterpart.
    """

    # Function to be passed to MultipleDataErrors.catch_all
    def ensure_cached(arg: tuple[str, Resource]) -> tuple[str, ManagedResource]:
        name, res = arg
        return _ensure_resource_cached(res, name, workspace)

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
    """prepare_resources ensures all provided Resources are available locally.

    If from_cache is False, missing or stale resources are fetched; otherwise
    MultipleDataError with ResourceNotCached may be raised.

    First returned element is a mapping from resource name
    to its ManagedResource counterpart.

    The second returned flag, indicating whether the Pipeline should continue.
    It's set to True if there are no resources, from_cache is True or at least
    one Resource was cached.
    """
    if not r:
        return {}, True
    elif from_cache:
        # Asked not to download any resources - just ensure they're all cached
        return ensure_resources_cached(r, workspace), True
    else:
        return cache_resources(r, workspace)
