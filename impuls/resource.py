import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
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


class ResourceManager:
    def __init__(self, resources: list[Resource]) -> None:
        self.remote: dict[str, Resource] = {i.name: i for i in resources}
        self.cached: dict[str, LocalResource] = {}
        self.logger: logging.Logger = logging.getLogger("ResourceManager")

    def cache_resources(self, workspace_dir: Path, ignore_not_modified: bool) -> None:
        # FIXME: Properly handle `ignore_not_modified`
        # - it should be raised if **all** resources were not modified.
        # In other words, if at least one resource has changed - the pipeline should run anyway.
        if not ignore_not_modified:
            raise NotImplementedError("Support for (ignore_not_modified = False) is broken")

        self.logger.info("Caching all resources")
        for resource in self.remote.values():
            self.logger.debug(f"Caching resource {resource.name}")
            self.cached[resource.name] = ensure_resource_downloaded(
                resource,
                workspace_dir,
                ignore_not_modified,
            )

    def get_resource_path(self, resource_name: str) -> Path:
        return self.cached[resource_name].file

    def get_resource_download_time(self, resource_name: str) -> datetime:
        return self.cached[resource_name].date_modified()

    def get_resource_modified_time(self, resource_name: str) -> datetime:
        return self.remote[resource_name].date_modified()


def ensure_resource_downloaded(
    resource: Resource, workspace_dir: Path, ignore_not_modified: bool
) -> LocalResource:
    """Ensures that a resource is cached, and returns a LocalResource representing
    the cached version."""
    # Don't cache local resources to avoid file copying
    if isinstance(resource, LocalResource):
        return resource

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

    return LocalResource(resource.name, cached_file)
