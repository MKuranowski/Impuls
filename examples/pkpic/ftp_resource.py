from datetime import datetime, timezone
from ftplib import FTP_TLS
from typing import Iterator

from impuls.errors import InputNotModified
from impuls.resource import ConcreteResource

FTP_URL = "ftps.intercity.pl"


class PatchedFTP(FTP_TLS):
    def makepasv(self) -> tuple[str, int]:
        """Parse PASV response, but ignore provided IP. PKP IC's FTP sends incorrect addresses."""
        _, port = super().makepasv()
        return self.host, port

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
    def __init__(
        self,
        filename: str,
        username: str,
        password: str,
    ) -> None:
        super().__init__()
        self.filename = filename
        self.username = username
        self.password = password

    def fetch(self, conditional: bool) -> Iterator[bytes]:
        with PatchedFTP(FTP_URL, self.username, self.password) as ftp:
            ftp.prot_p()

            current_last_modified = ftp.mod_time(self.filename)
            if conditional and current_last_modified <= self.last_modified:
                raise InputNotModified

            self.last_modified = current_last_modified
            self.fetch_time = datetime.now(timezone.utc)
            yield from ftp.iter_binary(f"RETR {self.filename}")
