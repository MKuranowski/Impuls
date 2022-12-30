import logging
import re
import sys
import unittest
from io import StringIO
from typing import TYPE_CHECKING
from unittest.mock import patch

from impuls.tools import color, logs


class TestInitializeLogging(unittest.TestCase):
    fake_root_logger: logging.Logger

    def setUp(self) -> None:
        self.fake_root_logger = logging.Logger("FakeRootLogger")

    def fake_get_logger(self, _name: str = "") -> logging.Logger:
        return self.fake_root_logger

    def test_adds_handler(self):
        # Assume there are no handlers at the beginning
        self.assertEqual(len(self.fake_root_logger.handlers), 0)

        # Initialize the logging
        with patch("logging.getLogger", self.fake_get_logger):
            logs.initialize(verbose=False)

        # Check that a handler was added
        self.assertEqual(len(self.fake_root_logger.handlers), 1)

        # Check that the handler is a StreamHandler
        added_handler = self.fake_root_logger.handlers[0]
        self.assertIsInstance(added_handler, logging.StreamHandler)
        if TYPE_CHECKING:
            assert isinstance(added_handler, logging.StreamHandler)

        # Check that the handler's stream is stderr
        self.assertIs(added_handler.stream, sys.stderr)  # type: ignore

        # Check that the handler's formatter is a ColoredFormatter
        self.assertIsInstance(added_handler.formatter, logs.ColoredFormatter)

    def test_removes_handlers(self):
        # Assume there are no handlers at the beginning
        self.assertEqual(len(self.fake_root_logger.handlers), 0)

        # Prepare 4 handlers - 2 should be removed, 2 retained
        handler_removed_1 = logging.StreamHandler(sys.stdout)
        handler_removed_2 = logging.StreamHandler(sys.stderr)
        handler_retained_1 = logging.StreamHandler(StringIO())
        handler_retained_2 = logging.NullHandler()

        # Add the handlers
        self.fake_root_logger.addHandler(handler_removed_1)
        self.fake_root_logger.addHandler(handler_removed_2)
        self.fake_root_logger.addHandler(handler_retained_1)
        self.fake_root_logger.addHandler(handler_retained_2)

        # Initialize the logging
        with patch("logging.getLogger", self.fake_get_logger):
            logs.initialize(verbose=False)

        # Check if handlers were removed properly
        self.assertNotIn(
            handler_removed_1,
            self.fake_root_logger.handlers,
            "StreamHandler(stdout) should have been removed by logs.initialize",
        )
        self.assertNotIn(
            handler_removed_2,
            self.fake_root_logger.handlers,
            "StreamHandler(stderr) should have been removed by logs.initialize",
        )

        # Check if handlers were removed properly
        self.assertIn(
            handler_retained_1,
            self.fake_root_logger.handlers,
            "StreamHandler(StringIO) should not have been removed by logs.initialize",
        )
        self.assertIn(
            handler_retained_1,
            self.fake_root_logger.handlers,
            "NullHandler() should not have been removed by logs.initialize",
        )


class TestColoredFormatter(unittest.TestCase):
    LOGGER_NAME = "SomeLogger"

    def setUp(self) -> None:
        self.output = StringIO()
        self.logger = logging.Logger(self.LOGGER_NAME)
        handler = logging.StreamHandler(self.output)
        handler.setFormatter(logs.ColoredFormatter())
        self.logger.addHandler(handler)

    def expected_msg_format(self, level: str, msg_color: str, msg: str) -> str:
        blue_e = re.escape(color.BLUE)
        cyan_e = re.escape(color.CYAN)
        green_e = re.escape(color.GREEN)
        reset_e = re.escape(color.RESET)

        logger_e = re.escape(self.LOGGER_NAME)
        level_e = re.escape(level)
        msg_color_e = re.escape(msg_color)
        msg_e = re.escape(msg)

        return (
            rf"{blue_e}\[{cyan_e}{level_e}{blue_e} \d\d?:\d\d:\d\d\.\d\d\d] "
            rf"{green_e}{logger_e}{reset_e}: {msg_color_e}{msg_e}{reset_e}"
        )

    def test_debug(self) -> None:
        self.logger.debug("Hello world")
        self.assertRegex(
            self.output.getvalue(),
            self.expected_msg_format("DEBUG", color.DIM, "Hello world"),
        )

    def test_info(self) -> None:
        self.logger.info("Hello world")
        self.assertRegex(
            self.output.getvalue(),
            self.expected_msg_format("INFO", color.RESET, "Hello world"),
        )

    def test_warning(self) -> None:
        self.logger.warning("Hello world")
        self.assertRegex(
            self.output.getvalue(),
            self.expected_msg_format("WARNING", color.YELLOW, "Hello world"),
        )

    def test_error(self) -> None:
        self.logger.error("Hello world")
        self.assertRegex(
            self.output.getvalue(),
            self.expected_msg_format("ERROR", color.RED, "Hello world"),
        )

    def test_critical(self) -> None:
        self.logger.critical("Hello world")
        self.assertRegex(
            self.output.getvalue(),
            self.expected_msg_format("CRITICAL", color.WHITE + color.BG_RED, "Hello world"),
        )
