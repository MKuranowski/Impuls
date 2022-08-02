# pyright: reportConstantRedefinition=false
import os

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
    RESET = "\x1B[0m"
    BOLD = "\x1B[1m"
    DIM = "\x1B[2m"

    BLACK = "\x1B[30m"
    RED = "\x1B[31m"
    GREEN = "\x1B[32m"
    YELLOW = "\x1B[33m"
    BLUE = "\x1B[34m"
    MAGENTA = "\x1B[35m"
    CYAN = "\x1B[36m"
    WHITE = "\x1B[37m"

    BG_BLACK = "\x1B[40m"
    BG_RED = "\x1B[41m"
    BG_GREEN = "\x1B[42m"
    BG_YELLOW = "\x1B[43m"
    BG_BLUE = "\x1B[44m"
    BG_MAGENTA = "\x1B[45m"
    BG_CYAN = "\x1B[46m"
    BG_WHITE = "\x1B[47m"
