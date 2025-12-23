# © Copyright 2022-2025 Mikołaj Kuranowski
# SPDX-License-Identifier: GPL-3.0-or-later

# pyright: reportConstantRedefinition=false
import os
import re
from typing import Literal

if os.getenv("NO_COLOR"):
    RESET = ""
    BOLD = ""
    DIM = ""

    BLACK = ""
    RED = ""
    GREEN = ""
    YELLOW = ""
    BLUE = ""
    MAGENTA = ""
    CYAN = ""
    WHITE = ""

    BG_BLACK = ""
    BG_RED = ""
    BG_GREEN = ""
    BG_YELLOW = ""
    BG_BLUE = ""
    BG_MAGENTA = ""
    BG_CYAN = ""
    BG_WHITE = ""

else:
    RESET = "\x1b[0m"
    BOLD = "\x1b[1m"
    DIM = "\x1b[2m"

    BLACK = "\x1b[30m"
    RED = "\x1b[31m"
    GREEN = "\x1b[32m"
    YELLOW = "\x1b[33m"
    BLUE = "\x1b[34m"
    MAGENTA = "\x1b[35m"
    CYAN = "\x1b[36m"
    WHITE = "\x1b[37m"

    BG_BLACK = "\x1b[40m"
    BG_RED = "\x1b[41m"
    BG_GREEN = "\x1b[42m"
    BG_YELLOW = "\x1b[43m"
    BG_BLUE = "\x1b[44m"
    BG_MAGENTA = "\x1b[45m"
    BG_CYAN = "\x1b[46m"
    BG_WHITE = "\x1b[47m"


def text_color_for(color: str) -> Literal["000000", "FFFFFF"]:
    """Estimates a text color which would contrast better when written
    over a background with the given `color`. Always returns either "000000" (black)
    or "FFFFFF" (white). Input must be a six-digit hex color, optionally prefixed by a "#".

    >>> text_color_for("000000")
    'FFFFFF'
    >>> text_color_for("FFFFFF")
    '000000'
    >>> text_color_for("#bb0000")
    'FFFFFF'
    >>> text_color_for("#ddaa00")
    '000000'
    """
    m = re.match(r"^#?([0-9A-Fa-f]{2})([0-9A-Fa-f]{2})([0-9A-Fa-f]{2})$", color)
    if not m:
        raise ValueError(f"invalid 6-digit hex color: {color!r}")

    r = int(m[1], base=16)
    g = int(m[2], base=16)
    b = int(m[3], base=16)
    yiq = 0.299 * r + 0.587 * g + 0.114 * b
    return "000000" if yiq > 128 else "FFFFFF"
