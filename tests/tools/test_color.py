import importlib
import os
import unittest
from unittest.mock import patch

from impuls.tools import color


class TestColors(unittest.TestCase):
    @patch.dict(os.environ, {"NO_COLOR": ""})
    def test(self) -> None:
        importlib.reload(color)

        self.assertNotEqual(color.RED, "")
        self.assertNotEqual(color.RESET, "")

    @patch.dict(os.environ, {"NO_COLOR": "1"})
    def test_respects_no_color(self) -> None:
        importlib.reload(color)

        self.assertEqual(color.RED, "")
        self.assertEqual(color.RESET, "")
