from dataclasses import dataclass
from email.utils import parsedate_to_datetime
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator, Protocol
import requests


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
            while (chunk := f.read(8192)):
                yield chunk
