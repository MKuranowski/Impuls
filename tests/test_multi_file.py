import logging
from datetime import datetime
from pathlib import Path
from typing import cast
from unittest import TestCase
from unittest.mock import Mock, patch

from impuls import LocalResource, Pipeline
from impuls.model import Date
from impuls.multi_file import IntermediateFeed, Pipelines, _ResolvedVersions, logger
from impuls.tools.testing_mocks import MockResource


class TestPipelines(TestCase):
    @staticmethod
    def pipeline_with_mock_run() -> Pipeline:
        p = Pipeline([])
        p.run = Mock()
        return p

    def test_run(self) -> None:
        p = Pipelines(
            [self.pipeline_with_mock_run(), self.pipeline_with_mock_run()],
            self.pipeline_with_mock_run(),
        )
        p.run()

        cast(Mock, p.intermediates[0].run).assert_called_once()
        cast(Mock, p.intermediates[1].run).assert_called_once()
        cast(Mock, p.final.run).assert_called_once()


class TestIntermediateFeed(TestCase):
    def test_as_local_resource(self) -> None:
        f = IntermediateFeed(
            MockResource(
                b"",
                datetime.fromisoformat("2023-04-02T12:57:00Z"),
                datetime.fromisoformat("2023-04-01T11:10:00Z"),
            ),
            "foo_v1.txt",
            "v1",
            Date(2023, 4, 1),
        )
        p = Path("/tmp/foo_v1.txt")
        local_f = f.as_local_resource(p)

        self.assertEqual(local_f.resource.path, p)
        self.assertEqual(
            local_f.resource.fetch_time,
            datetime.fromisoformat("2023-04-02T12:57:00Z"),
        )
        self.assertEqual(
            local_f.resource.last_modified,
            datetime.fromisoformat("2023-04-01T11:10:00Z"),
        )
        self.assertEqual(local_f.resource_name, "foo_v1.txt")
        self.assertEqual(local_f.version, "v1")
        self.assertEqual(local_f.start_date, Date(2023, 4, 1))

    def test_as_cached_metadata(self) -> None:
        f = IntermediateFeed(
            MockResource(
                b"",
                datetime.fromisoformat("2023-04-02T12:57:00Z"),
                datetime.fromisoformat("2023-04-01T11:10:00Z"),
            ),
            "foo_v1.txt",
            "v1",
            Date(2023, 4, 1),
        )
        d = f.as_cached_feed_metadata()

        self.assertDictEqual(
            d,
            {
                "version": "v1",
                "start_date": "2023-04-01",
                "last_modified": 1680347400.0,
                "fetch_time": 1680440220.0,
            },
        )

    def test_from_cached_metadata(self) -> None:
        p = Path("/tmp/foo_v1.txt")
        f = IntermediateFeed.from_cached_feed_metadata(
            LocalResource(p),
            {
                "version": "v1",
                "start_date": "2023-04-01",
                "last_modified": 1680347400.0,
                "fetch_time": 1680440220.0,
            },
        )

        self.assertEqual(f.resource.path, p)
        self.assertEqual(
            f.resource.fetch_time,
            datetime.fromisoformat("2023-04-02T12:57:00Z"),
        )
        self.assertEqual(
            f.resource.last_modified,
            datetime.fromisoformat("2023-04-01T11:10:00Z"),
        )
        self.assertEqual(f.resource_name, "foo_v1.txt")
        self.assertEqual(f.version, "v1")
        self.assertEqual(f.start_date, Date(2023, 4, 1))


class TestResolvedVersions(TestCase):
    def test_log_result_zero(self) -> None:
        r = _ResolvedVersions()  # type: ignore
        with self.assertLogs(logger, logging.INFO) as logs:
            r.log_result()
        self.assertListEqual(
            logs.output,
            [
                "INFO:MultiFile:0 cached input feeds are stale",
                "INFO:MultiFile:0 cached input feeds are up-to-date",
                "INFO:MultiFile:0 input feeds need to be downloaded",
            ],
        )

    def test_log_result_one(self) -> None:
        r = _ResolvedVersions(
            [IntermediateFeed(LocalResource(Path()), "v1.txt", "v1", Date(2023, 4, 1))],
            [IntermediateFeed(LocalResource(Path()), "v2.txt", "v2", Date(2023, 5, 1))],
            [IntermediateFeed(MockResource(), "v3.txt", "v3", Date(2023, 6, 1))],
        )
        with self.assertLogs(logger, logging.INFO) as logs:
            r.log_result()
        self.assertListEqual(
            logs.output,
            [
                "INFO:MultiFile:1 cached input feed is stale:\n\tv1.txt",
                "INFO:MultiFile:1 cached input feed is up-to-date:\n\tv2.txt",
                "INFO:MultiFile:1 input feed needs to be downloaded:\n\tv3.txt",
            ],
        )

    def test_log_result_many(self) -> None:
        r = _ResolvedVersions(
            [
                IntermediateFeed(LocalResource(Path()), "v1.txt", "v1", Date(2023, 4, 1)),
                IntermediateFeed(LocalResource(Path()), "v2.txt", "v2", Date(2023, 5, 1)),
            ],
            [
                IntermediateFeed(LocalResource(Path()), "v3.txt", "v3", Date(2023, 6, 1)),
                IntermediateFeed(LocalResource(Path()), "v4.txt", "v4", Date(2023, 7, 1)),
            ],
            [
                IntermediateFeed(MockResource(), "v5.txt", "v5", Date(2023, 8, 1)),
                IntermediateFeed(MockResource(), "v6.txt", "v6", Date(2023, 9, 1)),
            ],
        )
        with self.assertLogs(logger, logging.INFO) as logs:
            r.log_result()
        self.assertListEqual(
            logs.output,
            [
                "INFO:MultiFile:2 cached input feeds are stale:\n\tv1.txt, v2.txt",
                "INFO:MultiFile:2 cached input feeds are up-to-date:\n\tv3.txt, v4.txt",
                "INFO:MultiFile:2 input feeds need to be downloaded:\n\tv5.txt, v6.txt",
            ],
        )

    def test_remove(self) -> None:
        d = Path("/tmp/non-existing")
        r = _ResolvedVersions(
            [
                IntermediateFeed(LocalResource(d / "v1.txt"), "v1.txt", "v1", Date(2023, 4, 1)),
                IntermediateFeed(LocalResource(d / "v2.txt"), "v2.txt", "v2", Date(2023, 5, 1)),
            ],
            [IntermediateFeed(LocalResource(d / "v3.txt"), "v3.txt", "v3", Date(2023, 6, 1))],
            [IntermediateFeed(MockResource(), "v4.txt", "v4", Date(2023, 7, 1))],
        )
        with patch("impuls.multi_file._remove_from_cache") as remove_mock:
            r.remove(d)

        self.assertEqual(remove_mock.call_count, 2)

        self.assertEqual(remove_mock.mock_calls[0].args[0], d)
        self.assertIsInstance(remove_mock.mock_calls[0].args[1], IntermediateFeed)
        self.assertEqual(remove_mock.mock_calls[0].args[1].version, "v1")

        self.assertEqual(remove_mock.mock_calls[1].args[0], d)
        self.assertIsInstance(remove_mock.mock_calls[1].args[1], IntermediateFeed)
        self.assertEqual(remove_mock.mock_calls[1].args[1].version, "v2")
