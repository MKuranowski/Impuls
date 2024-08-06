from datetime import datetime, timezone
from ftplib import FTP
from typing import Iterator

from impuls.errors import InputNotModified
from impuls.model import Date
from impuls.multi_file import IntermediateFeed, IntermediateFeedProvider, prune_outdated_feeds
from impuls.resource import ConcreteResource

FTP_URL = "rozklady.ztm.waw.pl"


class PatchedFTP(FTP):
    def mod_time(self, filename: str) -> datetime:
        resp = self.voidcmd(f"MDTM {filename}")
        return self.parse_ftp_mod_time(resp.partition(" ")[2])

    def iter_binary(self, cmd: str, blocksize: int = 8192) -> Iterator[bytes]:
        # See the implementation of FTP.retrbinary. This is the same, but instead of
        # using the callback we just yield the data.
        self.voidcmd("TYPE I")
        with self.transfercmd(cmd) as conn:
            while data := conn.recv(blocksize):
                yield data
        return self.voidresp()

    @staticmethod
    def parse_ftp_mod_time(x: str) -> datetime:
        if len(x) == 14:
            return datetime.strptime(x, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
        elif len(x) > 15:
            return datetime.strptime(x[:21], "%Y%m%d%H%M%S.%f").replace(tzinfo=timezone.utc)
        else:
            raise ValueError(f"invalid FTP mod_time: {x}")


class FTPResource(ConcreteResource):
    def __init__(self, filename: str) -> None:
        super().__init__()
        self.filename = filename

    def fetch(self, conditional: bool) -> Iterator[bytes]:
        with PatchedFTP(FTP_URL) as ftp:
            ftp.login()

            current_last_modified = ftp.mod_time(self.filename)
            if conditional and current_last_modified <= self.last_modified:
                raise InputNotModified

            self.last_modified = current_last_modified
            self.fetch_time = datetime.now(timezone.utc)
            yield from ftp.iter_binary(f"RETR {self.filename}")


class ZTMFeedProvider(IntermediateFeedProvider[FTPResource]):
    def __init__(self, for_date: Date | None = None) -> None:
        self.for_date = Date.today()

    def needed(self) -> list[IntermediateFeed[FTPResource]]:
        with PatchedFTP("rozklady.ztm.waw.pl") as ftp:
            ftp.login()

            # Retrieve all feeds from the FTP
            all_feeds = [
                IntermediateFeed(
                    resource=FTPResource(
                        filename,
                    ),
                    resource_name=filename,
                    version=filename.partition(".")[0],
                    start_date=Date(
                        2000 + int(filename[2:4]),
                        int(filename[4:6]),
                        int(filename[6:8]),
                    ),
                )
                for filename in ftp.nlst()
                if filename.startswith("RA") and filename.endswith(".7z")
            ]

            prune_outdated_feeds(all_feeds, self.for_date)
            return all_feeds
