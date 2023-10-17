from typing import cast
from unittest import TestCase
from unittest.mock import Mock

from impuls import Pipeline
from impuls.multi_file import Pipelines


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
