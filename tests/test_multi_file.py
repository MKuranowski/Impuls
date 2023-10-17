from datetime import datetime
from pathlib import Path
from typing import cast
from unittest import TestCase
from unittest.mock import Mock

from impuls import LocalResource, Pipeline
from impuls.model import Date
from impuls.multi_file import IntermediateFeed, Pipelines
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
