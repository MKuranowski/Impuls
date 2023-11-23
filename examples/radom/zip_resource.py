from datetime import datetime
from io import BytesIO
from typing import Iterator
from zipfile import ZipFile

from impuls.resource import FETCH_CHUNK_SIZE, Resource


class ZippedResource(Resource):
    def __init__(self, r: Resource) -> None:
        self.r = r

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
        buffer = self.fetch_archived(conditional)
        with ZipFile(buffer) as archive:
            files = archive.infolist()
            if len(files) != 1:
                raise ValueError("Expected one file in ZIP")

            with archive.open(files[0], mode="r") as file_buffer:
                while chunk := file_buffer.read(FETCH_CHUNK_SIZE):
                    yield chunk

    def fetch_archived(self, conditional: bool) -> BytesIO:
        b = BytesIO()
        for chunk in self.r.fetch(conditional):
            b.write(chunk)
        return b
