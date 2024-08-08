# © Copyright 2022-2024 Mikołaj Kuranowski
# SPDX-License-Identifier: GPL-3.0-or-later

import os
import sys

if __name__ == "__main__":
    target = sys.argv[1]
    if len(sys.argv) >= 3 and sys.argv[2] == "-Wl,--version":
        args = ["zig", "clang", "-fuse-ld=lld", *sys.argv[2:]]
    else:
        args = ["zig", "cc", f"--target={target}", *sys.argv[2:]]
    os.execvp("zig", args)
