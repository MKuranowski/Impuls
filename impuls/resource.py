from email.utils import parsedate_to_datetime
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator, NamedTuple, Protocol
import requests


class Resource(Protocol):
    @property
    def name(self) -> str:
        ...

    def date_modified(self) -> datetime:
        ...

    def fetch(self) -> Iterator[bytes]:
        ...


class HTTPResource(NamedTuple):
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


class LocalResource(NamedTuple):
    name: str
    file: Path

    def date_modified(self) -> datetime:
        return datetime.fromtimestamp(self.file.stat().st_mtime, timezone.utc)

    def fetch(self) -> Iterator[bytes]:
        with self.file.open(mode="rb") as f:
            while (chunk := f.read(8192)):
                yield chunk
