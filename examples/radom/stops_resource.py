import csv
from datetime import datetime, timezone
from io import BytesIO, TextIOWrapper
from typing import Any, Iterator, cast

import zeep

from impuls.resource import FETCH_CHUNK_SIZE, ConcreteResource


class RadomStopsResource(ConcreteResource):
    def fetch(self, conditional: bool) -> Iterator[bytes]:
        # Fetch stops from Radom's SOAP service
        self.fetch_time = datetime.now(timezone.utc)
        client = zeep.Client("http://rkm.mzdik.radom.pl/PublicService.asmx?WSDL")
        service = client.create_service(  # type: ignore
            r"{http://PublicService/}PublicServiceSoap",
            "http://rkm.mzdik.radom.pl/PublicService.asmx",
        )
        stops = cast(Any, service.GetGoogleStops().findall("S"))  # type: ignore

        if len(stops) == 0:
            raise RuntimeError("no stops returned from rkm.mzdik.radom.pl")

        # Dump the stops to a csv
        buffer = BytesIO()
        text_buffer = TextIOWrapper(buffer, encoding="utf-8", newline="")
        writer = csv.writer(text_buffer)
        writer.writerow(("stop_id", "stop_name", "stop_lat", "stop_lon"))
        for stop in stops:
            writer.writerow(
                (
                    stop.attrib["id"],
                    stop.get("n", "").strip(),
                    stop.get("y", ""),
                    stop.get("x", ""),
                )
            )
        text_buffer.flush()

        # Yield CSV data
        buffer.seek(0)
        while chunk := buffer.read(FETCH_CHUNK_SIZE):
            yield chunk
