from io import TextIOWrapper
from typing import IO, AnyStr
from zipfile import ZipFile

from impuls.model import Agency, Calendar, Date, ExtraTableRow
from impuls.tasks import SaveGTFS
from impuls.tools.testing_mocks import MockFile

from .template_testcase import AbstractTestTask


class TestSaveGTFS(AbstractTestTask.Template):
    db_name = "wkd.db"

    def test(self) -> None:
        with MockFile() as gtfs_path:
            t = SaveGTFS(
                headers={
                    "agency.txt": ("agency_id", "agency_name", "agency_timezone", "agency_url"),
                    "calendar.txt": (
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
                    "calendar_dates.txt": ("service_id", "date", "exception_type"),
                    "routes.txt": (
                        "route_id",
                        "agency_id",
                        "route_short_name",
                        "route_long_name",
                        "route_type",
                    ),
                    "stops.txt": (
                        "stop_id",
                        "stop_name",
                        "stop_lat",
                        "stop_lon",
                        "wheelchair_boarding",
                    ),
                    "trips.txt": (
                        "route_id",
                        "service_id",
                        "trip_id",
                        "trip_headsign",
                        "trip_short_name",
                        "wheelchair_accessible",
                    ),
                    "stop_times.txt": (
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
                self.assertEqual(header, ",".join(t.headers["agency.txt"]))
                self.assertEqual(
                    record,
                    "0,Warszawska Kolej Dojazdowa,Europe/Warsaw,http://www.wkd.com.pl/",
                )
                self.assertEqual(count, 1)

                header, record, count = header_first_record_and_record_count(gtfs, "calendar.txt")
                self.assertEqual(header, ",".join(t.headers["calendar.txt"]))
                self.assertEqual(record, "C,0,0,0,0,0,1,1,20230508,20240430")
                self.assertEqual(count, 2)

                header, record, count = header_first_record_and_record_count(
                    gtfs,
                    "calendar_dates.txt",
                )
                self.assertEqual(header, ",".join(t.headers["calendar_dates.txt"]))
                self.assertEqual(record, "D,20230608,2")
                self.assertEqual(count, 14)

                header, record, count = header_first_record_and_record_count(gtfs, "routes.txt")
                self.assertEqual(header, ",".join(t.headers["routes.txt"]))
                self.assertEqual(
                    record,
                    "A1,0,A1,Warszawa Śródmieście WKD — Grodzisk Mazowiecki Radońska,2",
                )
                self.assertEqual(count, 3)

                header, record, count = header_first_record_and_record_count(gtfs, "stops.txt")
                self.assertEqual(header, ",".join(t.headers["stops.txt"]))
                self.assertEqual(
                    record,
                    "wsrod,Warszawa Śródmieście WKD,52.22768605033,21.00040372159,2",
                )
                self.assertEqual(count, 28)

                header, record, count = header_first_record_and_record_count(gtfs, "trips.txt")
                self.assertEqual(header, ",".join(t.headers["trips.txt"]))
                self.assertEqual(record, "A1,C,C-303,Podkowa Leśna Główna,303,1")
                self.assertEqual(count, 372)

                header, record, count = header_first_record_and_record_count(
                    gtfs,
                    "stop_times.txt",
                )
                self.assertEqual(header, ",".join(t.headers["stop_times.txt"]))
                self.assertEqual(record, "C-303,0,wsrod,05:05:00,05:05:00")
                self.assertEqual(count, 6276)

    def test_ensure_order(self) -> None:
        with MockFile() as gtfs_path:
            t = SaveGTFS(headers={"stops.txt": ("stop_id",)}, target=gtfs_path, ensure_order=True)
            t.execute(self.runtime)

            with ZipFile(gtfs_path, "r") as gtfs:
                ids = gtfs.read("stops.txt").decode("utf-8-sig").splitlines()[1:]
                self.assertListEqual(ids, sorted(ids))


class TestSaveGTFSEmitEmptyCalendars(AbstractTestTask.Template):
    db_name = None

    def setUp(self) -> None:
        super().setUp()
        self.runtime.db.create(
            Calendar(
                "0",
                monday=False,
                tuesday=False,
                wednesday=False,
                thursday=False,
                friday=False,
                saturday=False,
                sunday=False,
                start_date=Date.SIGNALS_EXCEPTIONS,
                end_date=Date.SIGNALS_EXCEPTIONS,
            )
        )

    def test_set_to_false(self) -> None:
        with MockFile() as gtfs_path:
            t = SaveGTFS(
                headers={
                    "calendar.txt": (
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
                },
                target=gtfs_path,
                emit_empty_calendars=False,
            )

            t.execute(self.runtime)

            with ZipFile(gtfs_path, mode="r") as gtfs:
                header, record, count = header_first_record_and_record_count(gtfs, "calendar.txt")
                self.assertEqual(header, ",".join(t.headers["calendar.txt"]))
                self.assertEqual(record, "")
                self.assertEqual(count, 0)

    def test_set_to_true(self) -> None:
        with MockFile() as gtfs_path:
            t = SaveGTFS(
                headers={
                    "calendar.txt": (
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
                },
                target=gtfs_path,
                emit_empty_calendars=True,
            )

            t.execute(self.runtime)

            with ZipFile(gtfs_path, mode="r") as gtfs:
                header, record, count = header_first_record_and_record_count(gtfs, "calendar.txt")
                self.assertEqual(header, ",".join(t.headers["calendar.txt"]))
                self.assertEqual(record, "0,0,0,0,0,0,0,0,11111111,11111111")
                self.assertEqual(count, 1)


class TestSaveGTFSWithExtraFields(AbstractTestTask.Template):
    db_name = None

    def test(self) -> None:
        self.runtime.db.create_many(
            Agency,
            (
                Agency(
                    "0",
                    "Foo",
                    "https://foo.example.com",
                    "UTC",
                    extra_fields_json=r'{"agency_email":"foo@example.com","main_agency":"1"}',
                ),
                Agency(
                    "1",
                    "Bar",
                    "https://bar.example.com",
                    "UTC",
                    extra_fields_json=r'{"agency_email":"bar@example.com"}',
                ),
                Agency("2", "Baz", "https://baz.example.com", "UTC"),
            ),
        )

        with MockFile() as gtfs_path:
            t = SaveGTFS(
                headers={
                    "agency.txt": (
                        "agency_id",
                        "agency_name",
                        "agency_timezone",
                        "agency_url",
                        "agency_email",
                    ),
                },
                target=gtfs_path,
            )

            t.execute(self.runtime)

            with ZipFile(gtfs_path, mode="r") as gtfs:
                with gtfs.open("agency.txt", "r") as f:
                    content = TextIOWrapper(f, "utf-8", newline="").readlines()

            self.assertListEqual(
                content,
                [
                    "agency_id,agency_name,agency_timezone,agency_url,agency_email\r\n",
                    "0,Foo,UTC,https://foo.example.com,foo@example.com\r\n",
                    "1,Bar,UTC,https://bar.example.com,bar@example.com\r\n",
                    "2,Baz,UTC,https://baz.example.com,\r\n",
                ],
            )


class TestSaveGTFSWithExtraFiles(AbstractTestTask.Template):
    db_name = None

    def test(self) -> None:
        self.runtime.db.create_many(
            ExtraTableRow,
            (
                ExtraTableRow(0, "foo.txt", r'{"foo":"1","bar":"Hello"}', 0),
                ExtraTableRow(0, "foo.txt", r'{"foo":"2","bar":"World"}', 1),
                ExtraTableRow(0, "foo.txt", r'{"foo":"3"}', 2),
                ExtraTableRow(0, "bar.txt", r'{"spam":"eggs"}', 0),
            ),
        )

        with MockFile() as gtfs_path:
            t = SaveGTFS(
                headers={
                    "foo.txt": ("foo", "bar", "spam"),
                },
                target=gtfs_path,
            )

            t.execute(self.runtime)

            with ZipFile(gtfs_path, mode="r") as gtfs:
                with gtfs.open("foo.txt", "r") as f:
                    content = TextIOWrapper(f, "utf-8", newline="").readlines()

            self.assertListEqual(
                content,
                [
                    "foo,bar,spam\r\n",
                    "1,Hello,\r\n",
                    "2,World,\r\n",
                    "3,,\r\n",
                ],
            )


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
