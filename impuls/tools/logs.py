# © Copyright 2022-2024 Mikołaj Kuranowski
# SPDX-License-Identifier: GPL-3.0-or-later

import logging
import sys

from . import color


class ColoredFormatter(logging.Formatter):
    """ColoredFormatter is an opinionated log formatter with human-readable output colored
    with `ANSI escape sequences <https://en.wikipedia.org/wiki/ANSI_escape_code>`_.
    """

    default_time_format = "%H:%M:%S"
    default_msec_format = "%s.%03d"

    def __init__(self) -> None:
        super().__init__()

    @staticmethod
    def get_msg_color(level: int) -> str:
        if level >= logging.CRITICAL:
            return color.WHITE + color.BG_RED
        elif level >= logging.ERROR:
            return color.RED
        elif level >= logging.WARNING:
            return color.YELLOW
        elif level >= logging.INFO:
            # White is usually slightly dimmed than the normal style
            return color.RESET
        else:
            return color.DIM

    def usesTime(self) -> bool:
        return True

    def format(self, record: logging.LogRecord) -> str:
        record.message = record.getMessage()
        record.asctime = self.formatTime(record)
        if record.exc_info and not record.exc_text:
            record.exc_text = self.formatException(record.exc_info)
            exception_suffix = f"\n{record.exc_text}"
        else:
            exception_suffix = ""

        msg_color = self.get_msg_color(record.levelno)
        return (
            f"{color.BLUE}[{color.CYAN}{record.levelname}{color.BLUE} {record.asctime}] "
            f"{color.GREEN}{record.name}{color.RESET}: {msg_color}{record.message}{color.RESET}"
            f"{exception_suffix}"
        )


def initialize(verbose: bool) -> None:
    """Resets logging handlers to ensure only a single, logging.StreamHandler using Impuls's
    custom :py:class:`~impuls.tools.logsColoredFormatter` outputs onto the terminal (via stderr).
    Any other registered logging.Handlers printing to stdout or stderr are removed.
    """
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG if verbose else logging.INFO)

    # Remove any loggers that dump onto stdout/stderr
    handlers_to_remove: list[logging.Handler] = [
        handler
        for handler in root_logger.handlers
        if isinstance(handler, logging.StreamHandler)
        and (handler.stream is sys.stdout or handler.stream is sys.stderr)  # type: ignore
    ]

    for handler in handlers_to_remove:
        root_logger.removeHandler(handler)

    # Add our own handler for stderr
    new_handler = logging.StreamHandler(sys.stderr)
    new_handler.setFormatter(ColoredFormatter())
    root_logger.addHandler(new_handler)
