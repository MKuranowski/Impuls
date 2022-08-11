from dataclasses import dataclass
from email.utils import parsedate_to_datetime
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator, Protocol
import requests

from .errors import InputNotModified


class Resource(Protocol):
    name: str

    def date_modified(self) -> datetime:
        ...

    def fetch(self) -> Iterator[bytes]:
        ...


@dataclass(frozen=True, slots=True)
class HTTPResource(Resource):
    name: str
    url: str

    def date_modified(self) -> datetime:
        r = requests.head(self.url)
        r.raise_for_status()
        return parsedate_to_datetime(r.headers["Last-Modified"])

    def fetch(self) -> Iterator[bytes]:
        with requests.get(self.url, stream=True) as r:
            r.raise_for_status()
            yield from r.iter_content(8192, decode_unicode=False)


@dataclass(frozen=True, slots=True)
class LocalResource(Resource):
    name: str
    file: Path

    def date_modified(self) -> datetime:
        return datetime.fromtimestamp(self.file.stat().st_mtime, timezone.utc)

    def fetch(self) -> Iterator[bytes]:
        with self.file.open(mode="rb") as f:
            while chunk := f.read(8192):
                yield chunk


def ensure_resource_downloaded(
    resource: Resource, workspace_dir: Path, ignore_not_modified: bool
) -> Path:
    """Ensures that a resource is cached, and returns a Path to the cached file.
    Raises InputNotModified, if appropriate."""
    # Get the path to the cached file
    cached_file = workspace_dir / f"input_{resource.name}"

    # Try to check when cached file was downloaded
    try:
        cached_modified_time = datetime.fromtimestamp(
            cached_file.stat().st_mtime,
            timezone.utc,
        )
    except FileNotFoundError:
        cached_modified_time = datetime.min.replace(tzinfo=timezone.utc)

    if cached_modified_time < resource.date_modified():
        # If the cached version is stale - update it
        with cached_file.open(mode="wb") as f:
            for chunk in resource.fetch():
                f.write(chunk)

    elif not ignore_not_modified:
        # If the cached version is not stale - raise InputNotModified (if asked)
        raise InputNotModified

    return cached_file
