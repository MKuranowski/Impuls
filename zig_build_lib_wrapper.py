# © Copyright 2022-2024 Mikołaj Kuranowski
# SPDX-License-Identifier: GPL-3.0-or-later

import argparse
import shutil
import subprocess
from pathlib import Path
from typing import List, Optional

if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("-o", "--output", type=Path)
    arg_parser.add_argument("zig_build_lib_args", nargs="*")
    args = arg_parser.parse_args()
    output: Optional[Path] = args.output
    zib_build_lib_args: List[str] = args.zig_build_lib_args

    zig_path = shutil.which("zig")
    if zig_path is None:
        raise RuntimeError("'zig' executable not found in PATH. Do you have zig installed?")

    subprocess.run([zig_path, "build-lib", *zib_build_lib_args], check=True)

    if output is not None and not output.exists():
        # XXX: Try to find the requested output file. Usually, meson expected "libZZZ.so",
        #      but zig produced "ZZZ.so", or vice versa.
        if output.name.startswith("lib"):
            alt_output = output.with_name(output.name[3:])
        else:
            alt_output = output.with_name("lib" + output.name)

        if not alt_output.exists():
            raise FileNotFoundError(
                f"Expected zig to produce {output} or {alt_output}, but neither was found"
            )

        alt_output.rename(output)
