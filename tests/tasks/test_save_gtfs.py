from io import TextIOWrapper
from typing import IO, AnyStr
from zipfile import ZipFile

from impuls.tasks import SaveGTFS
from impuls.tools.testing_mocks import MockFile

from .template_testcase import AbstractTestTask


class TestSaveGTFS(AbstractTestTask.Template):
    db_name = "wkd.db"

    def test(self) -> None:
        with MockFile() as gtfs_path:
            t = SaveGTFS(
                headers={
                    "agency": ("agency_id", "agency_name", "agency_timezone", "agency_url"),
                    "calendar": (
                        "service_id",
                        "monday",
                        "tuesday",
                        "wednesday",
                        "thursday",
                        "friday",
                        "saturday",
                        "sunday",
                        "start_date",
                        "end_date",
                    ),
                    "calendar_dates": ("service_id", "date", "exception_type"),
                    "routes": (
                        "route_id",
                        "agency_id",
                        "route_short_name",
                        "route_long_name",
                        "route_type",
                    ),
                    "stops": (
                        "stop_id",
                        "stop_name",
                        "stop_lat",
                        "stop_lon",
                        "wheelchair_boarding",
                    ),
                    "trips": (
                        "route_id",
                        "service_id",
                        "trip_id",
                        "trip_headsign",
                        "trip_short_name",
                        "wheelchair_accessible",
                    ),
                    "stop_times": (
                        "trip_id",
                        "stop_sequence",
                        "stop_id",
                        "arrival_time",
                        "departure_time",
                    ),
                },
                target=gtfs_path,
            )

            t.execute(self.runtime)

            with ZipFile(gtfs_path, mode="r") as gtfs:
                header, record, count = header_first_record_and_record_count(gtfs, "agency.txt")
                self.assertEqual(header, ",".join(t.headers["agency"]))
                self.assertEqual(
                    record,
                    "0,Warszawska Kolej Dojazdowa,Europe/Warsaw,http://www.wkd.com.pl/",
                )
                self.assertEqual(count, 1)

                header, record, count = header_first_record_and_record_count(gtfs, "calendar.txt")
                self.assertEqual(header, ",".join(t.headers["calendar"]))
                self.assertEqual(record, "C,0,0,0,0,0,1,1,20230508,20240430")
                self.assertEqual(count, 2)

                header, record, count = header_first_record_and_record_count(
                    gtfs,
                    "calendar_dates.txt",
                )
                self.assertEqual(header, ",".join(t.headers["calendar_dates"]))
                self.assertEqual(record, "D,20230608,2")
                self.assertEqual(count, 14)

                header, record, count = header_first_record_and_record_count(gtfs, "routes.txt")
                self.assertEqual(header, ",".join(t.headers["routes"]))
                self.assertEqual(
                    record,
                    "A1,0,A1,Warszawa Śródmieście WKD — Grodzisk Mazowiecki Radońska,2",
                )
                self.assertEqual(count, 3)

                header, record, count = header_first_record_and_record_count(gtfs, "stops.txt")
                self.assertEqual(header, ",".join(t.headers["stops"]))
                self.assertEqual(
                    record,
                    "wsrod,Warszawa Śródmieście WKD,52.22768605033,21.00040372159,2",
                )
                self.assertEqual(count, 28)

                header, record, count = header_first_record_and_record_count(gtfs, "trips.txt")
                self.assertEqual(header, ",".join(t.headers["trips"]))
                self.assertEqual(record, "A1,C,C-303,Podkowa Leśna Główna,303,1")
                self.assertEqual(count, 372)

                header, record, count = header_first_record_and_record_count(
                    gtfs,
                    "stop_times.txt",
                )
                self.assertEqual(header, ",".join(t.headers["stop_times"]))
                self.assertEqual(record, "C-303,0,wsrod,05:05:00,05:05:00")
                self.assertEqual(count, 6276)


def header_first_record_and_record_count(z: ZipFile, f: str) -> tuple[str, str, int]:
    with z.open(f) as raw_buffer:
        buffer = TextIOWrapper(raw_buffer, "utf-8", newline="\r\n")
        header = buffer.readline().rstrip("\r\n")
        first_record = buffer.readline().rstrip("\r\n")
        record_count = count_chars_in_stream(buffer, "\n") + 1 if first_record else 0
    return header, first_record, record_count


def count_chars_in_stream(stream: IO[AnyStr], char: AnyStr) -> int:
    assert len(char) == 1
    count = 0
    while chunk := stream.read(4096):
        count += chunk.count(char)
    return count
