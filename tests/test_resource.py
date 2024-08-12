import csv
import json
import os
import unittest
from abc import ABC, abstractmethod
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime, parsedate_to_datetime
from pathlib import Path
from time import sleep
from typing import Callable, Final, Iterable, Iterator
from unittest.mock import patch

import impuls.resource
from impuls.errors import InputNotModified, MultipleDataErrors, ResourceNotCached
from impuls.resource import (
    DATETIME_MIN_UTC,
    HTTPResource,
    LocalResource,
    ManagedResource,
    Resource,
    TimeLimitedResource,
)
from impuls.tools.testing_mocks import MockDatetimeNow, MockFile, MockHTTPResponse, MockResource

FIXTURES_DIR = Path(__file__).parent / "fixtures"


def read_all(it: Iterable[bytes]) -> bytes:
    return b"".join(it)


class MockExceptionResource(MockResource):
    def fetch(self, conditional: bool) -> Iterator[bytes]:
        yield b"Hello"
        raise IOError()


class AbstractTestResource:
    # NOTE: Nested classes are necessary to prevent abstract test cases
    #       from being discovered and run.
    #       See https://stackoverflow.com/a/50176291.

    class Template(ABC, unittest.TestCase):
        CONTENT: Final[bytes] = b"Hello, world!\n"

        @abstractmethod
        def get_resource(self) -> Resource:
            raise NotImplementedError

        @abstractmethod
        def refresh_resource(self) -> None:
            raise NotImplementedError

        def sleep_before_fetching(self) -> None:
            pass

        def assert_resource_fetched(self, msg: str, conditional: bool = True) -> None:
            self.assertEqual(read_all(self.get_resource().fetch(conditional)), self.CONTENT, msg)

        def assert_resource_not_fetched(self, msg: str) -> None:
            with self.assertRaises(InputNotModified, msg=msg):
                read_all(self.get_resource().fetch(conditional=True))

        def test(self) -> None:
            self.refresh_resource()
            self.assert_resource_fetched("1st fetch - after refresh")

            self.sleep_before_fetching()
            self.assert_resource_not_fetched("2nd fetch - no refresh")

            self.sleep_before_fetching()
            self.refresh_resource()
            self.assert_resource_fetched("3rd fetch - after refresh")

            self.sleep_before_fetching()
            self.assert_resource_not_fetched("4th fetch - no refresh")

            self.sleep_before_fetching()
            self.assert_resource_fetched("5th fetch - unconditional", conditional=False)


class TestLocalResource(AbstractTestResource.Template):
    def setUp(self) -> None:
        self.f = MockFile()
        self.r = LocalResource(self.f.path)

    def tearDown(self) -> None:
        self.f.cleanup()

    def get_resource(self) -> Resource:
        return self.r

    def refresh_resource(self) -> None:
        self.f.path.write_bytes(self.CONTENT)

    def sleep_before_fetching(self) -> None:
        sleep(0.01)


class TestHTTPResource(AbstractTestResource.Template):
    def setUp(self) -> None:
        self.mocked_dt = MockDatetimeNow.evenly_spaced(
            datetime(2023, 4, 1, 10, 0, tzinfo=timezone.utc),
            timedelta(seconds=30),
        )
        self.last_modified = DATETIME_MIN_UTC
        self.r = HTTPResource.get("https://localhost/hello")

    def get_resource(self) -> Resource:
        return self.r

    def refresh_resource(self) -> None:
        self.last_modified = self.mocked_dt.now()

    def prepare_mock_do_request(self) -> Callable[[HTTPResource], MockHTTPResponse]:
        def mock_do_response(r: HTTPResource) -> MockHTTPResponse:
            if_modified_since_str: str = r.request.headers.get("If-Modified-Since", "")
            if if_modified_since_str:
                if_modified_since = parsedate_to_datetime(if_modified_since_str)
                if if_modified_since >= self.last_modified:
                    return MockHTTPResponse(304)

            return MockHTTPResponse(
                200,
                self.CONTENT,
                {"Last-Modified": format_datetime(self.last_modified, usegmt=True)},
            )

        return mock_do_response

    def test(self) -> None:
        with (
            patch("impuls.resource.HTTPResource._do_request", self.prepare_mock_do_request()),
            self.mocked_dt.patch("impuls.resource.datetime"),
        ):
            super().test()


class TestHTTPEtagResource(AbstractTestResource.Template):
    def setUp(self) -> None:
        self.counter = 1
        self.r = HTTPResource.get("https://localhost/hello")

    def get_resource(self) -> Resource:
        return self.r

    def refresh_resource(self) -> None:
        self.counter += 1

    def prepare_mock_do_request(self) -> Callable[[HTTPResource], MockHTTPResponse]:
        def mock_do_response(r: HTTPResource) -> MockHTTPResponse:
            if_none_match: str = r.request.headers.get("If-None-Match", "")
            if if_none_match and int(if_none_match[1:-1]) == self.counter:
                return MockHTTPResponse(304)

            return MockHTTPResponse(
                200,
                self.CONTENT,
                {"ETag": f'"{self.counter}"'},
            )

        return mock_do_response

    def test(self) -> None:
        with patch("impuls.resource.HTTPResource._do_request", self.prepare_mock_do_request()):
            super().test()


class TestTimeLimitedResource(AbstractTestResource.Template):
    def setUp(self) -> None:
        self.mocked_dt = MockDatetimeNow(
            [
                datetime.fromisoformat("2023-04-01T10:00:00+00:00"),  # 1st call to refresh
                datetime.fromisoformat("2023-04-01T10:00:00+00:00"),  # 1st call to fetch (initial)
                datetime.fromisoformat("2023-04-01T10:00:00+00:00"),  # 1st fetchTime set
                datetime.fromisoformat("2023-04-01T10:00:15+00:00"),  # 2nd call to refresh
                datetime.fromisoformat("2023-04-01T10:00:30+00:00"),  # 2nd fetch (ltd.; changed)
                datetime.fromisoformat("2023-04-01T10:01:30+00:00"),  # 3rd fetch (n/ltd.; changed)
                datetime.fromisoformat("2023-04-01T10:01:30+00:00"),  # 2nd fetchTime set
                datetime.fromisoformat("2023-04-01T10:02:00+00:00"),  # 4th fetch (ltd.; unchanged)
                datetime.fromisoformat("2023-04-01T10:05:00+00:00"),  # 5th f. (n/ltd.; unchanged)
                datetime.fromisoformat("2023-04-01T10:06:00+00:00"),  # 6th fetch (unconditional)
                datetime.fromisoformat("2023-04-01T10:06:00+00:00"),  # 3rd fetchTime set
            ]
        )

        self.backing = MockResource(self.CONTENT, clock=self.mocked_dt.now)
        self.time_limited = TimeLimitedResource(self.backing, timedelta(minutes=1))

    def get_resource(self) -> Resource:
        return self.time_limited

    def refresh_resource(self) -> None:
        self.backing.refresh()

    def test(self) -> None:
        with self.mocked_dt.patch("impuls.resource.datetime"):
            self.refresh_resource()
            self.assert_resource_fetched("1st fetch - not time limited, changed")

            self.refresh_resource()
            self.assert_resource_not_fetched("2nd fetch - time limited, changed")
            self.assert_resource_fetched("3rd fetch - not time limited, changed")
            self.assert_resource_not_fetched("4th fetch - time limited, not changed")
            self.assert_resource_not_fetched("5th fetch - not time limited, not changed")
            self.assert_resource_fetched("6th fetch - unconditional", conditional=False)


class TestManagedResource(unittest.TestCase):
    def test_stat(self) -> None:
        r = ManagedResource(FIXTURES_DIR / "resource_local.txt")
        stat = r.stat()
        self.assertEqual(stat.st_size, 14)

    def test_size(self) -> None:
        r = ManagedResource(FIXTURES_DIR / "resource_local.txt")
        self.assertEqual(r.size(), 14)

    def test_open_text(self) -> None:
        r = ManagedResource(FIXTURES_DIR / "resource_local.txt")
        with r.open_text(encoding="ascii") as f:
            self.assertEqual(f.read(), "Hello, world!\n")

    def test_open_text_unicode(self) -> None:
        r = ManagedResource(FIXTURES_DIR / "resource_unicode.txt")
        with r.open_text(encoding="utf-8") as f:
            self.assertEqual(f.read(), "Zażółć gęślą jaźń\n")

    def test_open_binary(self) -> None:
        r = ManagedResource(FIXTURES_DIR / "resource_local.txt")
        with r.open_binary() as f:
            self.assertEqual(f.read(), b"Hello, world!\n")

    def test_text(self) -> None:
        r = ManagedResource(FIXTURES_DIR / "resource_local.txt")
        self.assertEqual(r.text(encoding="ascii"), "Hello, world!\n")

    def test_text_unicode(self) -> None:
        r = ManagedResource(FIXTURES_DIR / "resource_unicode.txt")
        self.assertEqual(r.text(encoding="utf-8-sig"), "Zażółć gęślą jaźń\n")

    def test_bytes(self) -> None:
        r = ManagedResource(FIXTURES_DIR / "resource_local.txt")
        self.assertEqual(r.bytes(), b"Hello, world!\n")

    def test_json(self) -> None:
        r = ManagedResource(FIXTURES_DIR / "resource_json.json")
        self.assertEqual(
            r.json(),
            {
                "message": "Hello, world",
                "ok": True,
            },
        )

    def test_yaml(self) -> None:
        r = ManagedResource(FIXTURES_DIR / "resource_yaml.yml")
        self.assertEqual(
            r.yaml(),
            {
                "message": "Hello, world!\n",
                "ok": True,
            },
        )

    def test_csv(self) -> None:
        r = ManagedResource(FIXTURES_DIR / "resource_csv.csv")
        rows = list(
            r.csv(
                encoding="utf-8",
                delimiter="\t",
                quotechar="'",
                quoting=csv.QUOTE_ALL,
            )
        )

        self.assertListEqual(
            rows,
            [
                {"City": "New York", "Stations": "424", "System Length": "380"},
                {"City": "Shanghai", "Stations": "345", "System Length": "676"},
                {"City": "Seoul", "Stations": "331", "System Length": "353"},
                {"City": "Beijing", "Stations": "326", "System Length": "690"},
                {"City": "Paris", "Stations": "302", "System Length": "214"},
                {"City": "London", "Stations": "270", "System Length": "402"},
            ],
        )


class TestResourceCaching(unittest.TestCase):
    def test_read_metadata(self) -> None:
        r = MockResource()
        impuls.resource._read_metadata(r, FIXTURES_DIR / "resource_metadata.json")
        self.assertEqual(r.last_modified, datetime.fromisoformat("2023-04-01T10:00:00+00:00"))
        self.assertEqual(r.fetch_time, datetime.fromisoformat("2023-04-01T10:08:12+00:00"))
        self.assertEqual(r.extra_metadata, {"foo": "bar"})

    def test_read_metadata_missing(self) -> None:
        r = MockResource()
        impuls.resource._read_metadata(r, FIXTURES_DIR / "resource_metadata_non_existing.json")
        self.assertEqual(r.last_modified, DATETIME_MIN_UTC)
        self.assertEqual(r.fetch_time, DATETIME_MIN_UTC)
        self.assertIsNone(r.extra_metadata)

    def test_write_metadata(self) -> None:
        with MockFile() as f:
            r = MockResource(
                last_modified=datetime.fromisoformat("2023-04-01T10:00:00+00:00"),
                fetch_time=datetime.fromisoformat("2023-04-01T10:08:12+00:00"),
                extra_metadata={"foo": "bar"},
            )
            impuls.resource._write_metadata(r, f)

            with f.open() as handle:
                self.assertDictEqual(
                    json.load(handle),
                    {
                        "last_modified": 1680343200.0,
                        "fetch_time": 1680343692.0,
                        "extra": {"foo": "bar"},
                    },
                )

    def test_download_resource(self) -> None:
        with MockFile() as path:
            r = MockResource(b"Hello, world!\n")
            r.refresh()
            impuls.resource._download_resource(r, path)
            self.assertEqual(path.read_bytes(), b"Hello, world!\n")

    def test_download_resource_exception(self) -> None:
        with MockFile() as path:
            path.write_bytes(b"Previously cached content\n")

            r = MockExceptionResource()
            r.refresh()
            with self.assertRaises(IOError):
                impuls.resource._download_resource(r, path)

            self.assertEqual(path.read_bytes(), b"Previously cached content\n")

    def test_cache_resources(self) -> None:
        with MockFile(directory=True) as workspace, MockFile() as local_resource_file:
            # Prepare the resources

            # 1. Resource which is already cached
            cached_resource = MockResource(b"Hello, world!\n")
            with (workspace / "cached.txt.metadata").open(mode="w") as f:
                json.dump(
                    {
                        "last_modified": datetime.fromisoformat(
                            "2023-04-01T11:30:00+00:00"
                        ).timestamp(),
                        "fetch_time": datetime.fromisoformat(
                            "2023-04-01T12:00:00+00:00"
                        ).timestamp(),
                    },
                    f,
                )
            (workspace / "cached.txt").write_bytes(b"Hello, world!\n")

            # 2. Resource which is cached, but outdated
            outdated_resource = MockResource(b"Hello, new world!\n")
            with (workspace / "outdated.txt.metadata").open(mode="w") as f:
                json.dump(
                    {
                        # NOTE: This is the mocked "new" last_modified of the resource.
                        #       Will be set in the outdated_resource by read_metadata call
                        #       within cache_resources.
                        "last_modified": datetime.fromisoformat(
                            "2023-04-01T13:30:00+00:00"
                        ).timestamp(),
                        # NOTE: This is a mocked "old" fetch_time
                        "fetch_time": datetime.fromisoformat(
                            "2023-04-01T08:00:00+00:00"
                        ).timestamp(),
                    },
                    f,
                )
            (workspace / "outdated.txt").write_bytes(b"Hello, world!\n")

            # 3. Resource which is missing
            missing_resource = MockResource(b"Lorem ipsum dolor sit amet\n")

            # 4. Local Resource
            local_resource_file.write_bytes(b"We the peoples of the United Nations\n")
            local_res_mod_timestamp = datetime.fromisoformat(
                "2023-04-01T22:00:00+00:00"
            ).timestamp()
            os.utime(local_resource_file, (local_res_mod_timestamp, local_res_mod_timestamp))
            local_resource = LocalResource(local_resource_file)

            # Cache the resources

            r, changed = impuls.resource.cache_resources(
                {
                    "cached.txt": cached_resource,
                    "outdated.txt": outdated_resource,
                    "missing.txt": missing_resource,
                    "local.txt": local_resource,
                },
                workspace,
            )

            # Check the resulting resources
            self.assertTrue(changed)

            # 1. Cached resource
            self.assertEqual(r["cached.txt"].stored_at, workspace / "cached.txt")
            self.assertEqual(r["cached.txt"].bytes(), b"Hello, world!\n")
            self.assertEqual(
                r["cached.txt"].last_modified,
                datetime.fromisoformat("2023-04-01T11:30:00+00:00"),
            )
            self.assertEqual(
                r["cached.txt"].fetch_time,
                datetime.fromisoformat("2023-04-01T12:00:00+00:00"),
            )

            # 2. Outdated resource
            self.assertEqual(r["outdated.txt"].stored_at, workspace / "outdated.txt")
            self.assertEqual(r["outdated.txt"].bytes(), b"Hello, new world!\n")
            self.assertEqual(
                r["outdated.txt"].last_modified,
                datetime.fromisoformat("2023-04-01T13:30:00+00:00"),
            )
            self.assertGreater(
                r["outdated.txt"].fetch_time,
                datetime.fromisoformat("2023-04-02T00:00:00+00:00"),
            )

            # 3. Missing resource
            self.assertEqual(r["missing.txt"].stored_at, workspace / "missing.txt")
            self.assertEqual(r["missing.txt"].bytes(), b"Lorem ipsum dolor sit amet\n")
            # NOTE: read_metadata breaks MockResource.last_modified
            # self.assertEqual(
            #     r["missing.txt"].last_modified,
            #     datetime.fromisoformat("2023-04-01T08:00:00+00:00"),
            # )
            self.assertGreater(
                r["missing.txt"].fetch_time,
                datetime.fromisoformat("2023-04-02T00:00:00+00:00"),
            )

            # 4. Local Resource
            self.assertEqual(r["local.txt"].stored_at, local_resource_file)
            self.assertEqual(r["local.txt"].bytes(), b"We the peoples of the United Nations\n")
            self.assertEqual(
                r["local.txt"].last_modified,
                datetime.fromisoformat("2023-04-01T22:00:00+00:00"),
            )
            self.assertEqual(
                r["local.txt"].fetch_time,
                datetime.fromisoformat("2023-04-01T22:00:00+00:00"),
            )

    def test_cache_resources_not_modified(self) -> None:
        with MockFile(directory=True) as workspace, MockFile() as local_resource_file:
            # Prepare resources

            # 1. Resource which is already cached
            cached_resource = MockResource(b"Hello, world!\n")
            with (workspace / "cached.txt.metadata").open(mode="w") as f:
                json.dump(
                    {
                        "last_modified": datetime.fromisoformat(
                            "2023-04-01T11:30:00+00:00"
                        ).timestamp(),
                        "fetch_time": datetime.fromisoformat(
                            "2023-04-01T12:00:00+00:00"
                        ).timestamp(),
                    },
                    f,
                )
            (workspace / "cached.txt").write_bytes(b"Hello, world!\n")

            # 2. Local Resource
            local_resource_file.write_bytes(b"We the peoples of the United Nations\n")
            local_res_mod_timestamp = datetime.fromisoformat(
                "2023-04-01T22:00:00+00:00"
            ).timestamp()
            os.utime(local_resource_file, (local_res_mod_timestamp, local_res_mod_timestamp))
            with (workspace / "local.txt.metadata").open(mode="w") as f:
                json.dump(
                    {
                        "last_modified": local_res_mod_timestamp,
                        "fetch_time": local_res_mod_timestamp,
                    },
                    f,
                )

            # Cache the resources
            _, changed = impuls.resource.cache_resources(
                {
                    "cached.txt": cached_resource,
                    "local.txt": LocalResource(local_resource_file),
                },
                workspace,
            )
            self.assertFalse(changed)

    def test_ensure_resources_cached_ok(self) -> None:
        with MockFile(directory=True) as workspace, MockFile() as local_resource_file:
            # Prepare the resources

            # 1. Some resource which was already fetched
            cached_resource = MockResource(b"Hello, world!\n")
            with (workspace / "cached.txt.metadata").open(mode="w") as f:
                json.dump(
                    {
                        "last_modified": datetime.fromisoformat(
                            "2023-04-01T11:30:00+00:00"
                        ).timestamp(),
                        "fetch_time": datetime.fromisoformat(
                            "2023-04-01T12:00:00+00:00"
                        ).timestamp(),
                    },
                    f,
                )
            (workspace / "cached.txt").write_bytes(b"Hello, world!\n")

            # 2. Local resource
            local_resource_file.write_bytes(b"We the peoples of the United Nations\n")
            local_res_mod_timestamp = datetime.fromisoformat(
                "2023-04-01T22:00:00+00:00"
            ).timestamp()
            os.utime(local_resource_file, (local_res_mod_timestamp, local_res_mod_timestamp))

            # Check if resources are cached

            r = impuls.resource.ensure_resources_cached(
                {
                    "cached.txt": cached_resource,
                    "local.txt": LocalResource(local_resource_file),
                },
                workspace,
            )

            # Check the resulting resources

            # 1. Cached resource
            self.assertEqual(r["cached.txt"].stored_at, workspace / "cached.txt")
            self.assertEqual(r["cached.txt"].bytes(), b"Hello, world!\n")
            self.assertEqual(
                r["cached.txt"].last_modified,
                datetime.fromisoformat("2023-04-01T11:30:00+00:00"),
            )
            self.assertEqual(
                r["cached.txt"].fetch_time,
                datetime.fromisoformat("2023-04-01T12:00:00+00:00"),
            )

            # 2. Local Resource
            self.assertEqual(r["local.txt"].stored_at, local_resource_file)
            self.assertEqual(r["local.txt"].bytes(), b"We the peoples of the United Nations\n")
            self.assertEqual(
                r["local.txt"].last_modified,
                datetime.fromisoformat("2023-04-01T22:00:00+00:00"),
            )
            self.assertEqual(
                r["local.txt"].fetch_time,
                datetime.fromisoformat("2023-04-01T22:00:00+00:00"),
            )

    def test_ensure_resources_cached_missing(self) -> None:
        with (
            MockFile(directory=True) as workspace,
            self.assertRaises(MultipleDataErrors) as caught,
        ):
            impuls.resource.ensure_resources_cached(
                {
                    "missing.txt": MockResource(),
                },
                workspace,
            )

        errors = caught.exception.errors
        self.assertEqual(len(errors), 1, "len(caught errors)")
        assert isinstance(errors[0], ResourceNotCached)  # for type checker
        self.assertEqual(errors[0].resource_name, "missing.txt")

    def test_ensure_resources_cached_missing_local(self) -> None:
        with (
            MockFile(directory=True) as workspace,
            self.assertRaises(MultipleDataErrors) as caught,
        ):
            impuls.resource.ensure_resources_cached(
                {
                    "missing_local.txt": LocalResource(FIXTURES_DIR / "non_existing.txt"),
                },
                workspace,
            )

        errors = caught.exception.errors
        self.assertEqual(len(errors), 1, "len(caught errors)")
        assert isinstance(errors[0], ResourceNotCached)  # for type checker
        self.assertEqual(errors[0].resource_name, "missing_local.txt")

    def test_prepare_resources_from_cache_ok(self) -> None:
        with MockFile(directory=True) as workspace, MockFile() as local_resource_file:
            # Prepare the resources

            # 1. Some resource which was already fetched
            cached_resource = MockResource(b"Hello, world!\n")
            with (workspace / "cached.txt.metadata").open(mode="w") as f:
                json.dump(
                    {
                        "last_modified": datetime.fromisoformat(
                            "2023-04-01T11:30:00+00:00"
                        ).timestamp(),
                        "fetch_time": datetime.fromisoformat(
                            "2023-04-01T12:00:00+00:00"
                        ).timestamp(),
                    },
                    f,
                )
            (workspace / "cached.txt").write_bytes(b"Hello, world!\n")

            # 2. Local resource
            local_resource_file.write_bytes(b"We the peoples of the United Nations\n")
            local_res_mod_timestamp = datetime.fromisoformat(
                "2023-04-01T22:00:00+00:00"
            ).timestamp()
            os.utime(local_resource_file, (local_res_mod_timestamp, local_res_mod_timestamp))

            # Check if resources are cached

            r, should_continue = impuls.resource.prepare_resources(
                {
                    "cached.txt": cached_resource,
                    "local.txt": LocalResource(local_resource_file),
                },
                workspace,
                from_cache=True,
            )

            # Check the resulting resources
            self.assertTrue(should_continue)

            # 1. Cached resource
            self.assertEqual(r["cached.txt"].stored_at, workspace / "cached.txt")
            self.assertEqual(r["cached.txt"].bytes(), b"Hello, world!\n")
            self.assertEqual(
                r["cached.txt"].last_modified,
                datetime.fromisoformat("2023-04-01T11:30:00+00:00"),
            )
            self.assertEqual(
                r["cached.txt"].fetch_time,
                datetime.fromisoformat("2023-04-01T12:00:00+00:00"),
            )

            # 2. Local Resource
            self.assertEqual(r["local.txt"].stored_at, local_resource_file)
            self.assertEqual(r["local.txt"].bytes(), b"We the peoples of the United Nations\n")
            self.assertEqual(
                r["local.txt"].last_modified,
                datetime.fromisoformat("2023-04-01T22:00:00+00:00"),
            )
            self.assertEqual(
                r["local.txt"].fetch_time,
                datetime.fromisoformat("2023-04-01T22:00:00+00:00"),
            )

    def test_prepare_resources_from_cache_missing(self) -> None:
        with (
            MockFile(directory=True) as workspace,
            self.assertRaises(MultipleDataErrors) as caught,
        ):
            impuls.resource.prepare_resources(
                {
                    "missing.txt": MockResource(),
                },
                workspace,
                from_cache=True,
            )

        errors = caught.exception.errors
        self.assertEqual(len(errors), 1, "len(caught errors)")
        assert isinstance(errors[0], ResourceNotCached)  # for type checker
        self.assertEqual(errors[0].resource_name, "missing.txt")

    def test_prepare_resources_fetches(self) -> None:
        with MockFile(directory=True) as workspace:
            r, should_continue = impuls.resource.prepare_resources(
                {"missing.txt": MockResource(b"Hello, world!\n")},
                workspace,
            )

            self.assertTrue(should_continue)
            self.assertEqual(r["missing.txt"].stored_at, workspace / "missing.txt")
            self.assertEqual(r["missing.txt"].bytes(), b"Hello, world!\n")

    def test_prepare_resources_raises_input_not_modified(self) -> None:
        with MockFile(directory=True) as workspace:
            cached_resource = MockResource(b"Hello, world!\n")
            with (workspace / "cached.txt.metadata").open(mode="w") as f:
                json.dump(
                    {
                        "last_modified": datetime.fromisoformat(
                            "2023-04-01T11:30:00+00:00"
                        ).timestamp(),
                        "fetch_time": datetime.fromisoformat(
                            "2023-04-01T12:00:00+00:00"
                        ).timestamp(),
                    },
                    f,
                )
            (workspace / "cached.txt").write_bytes(b"Hello, world!\n")

            r, should_continue = impuls.resource.prepare_resources(
                {"cached.txt": cached_resource},
                workspace,
            )

            self.assertFalse(should_continue)
            self.assertEqual(len(r), 1)
            self.assertEqual(r["cached.txt"].bytes(), b"Hello, world!\n")
