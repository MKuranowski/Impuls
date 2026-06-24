# © Copyright 2022-2026 Mikołaj Kuranowski
# SPDX-License-Identifier: GPL-3.0-or-later

import argparse
import os
import shutil
import subprocess
import sys
from contextlib import contextmanager
from pathlib import Path
from typing import Generator, List, Optional


@contextmanager
def cwd(target: Path) -> Generator[None, None, None]:
    current = Path.cwd()
    try:
        os.chdir(target)
        yield
    finally:
        os.chdir(current)


def cargo_build_type(args: List[str]) -> str:
    if "-r" in args or "--release" in args:
        return "release"
    return "debug"


if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("-c", "--cross", default="")
    arg_parser.add_argument("-o", "--output", type=Path)
    arg_parser.add_argument("-s", "--source-dir", type=Path)
    arg_parser.add_argument("-t", "--target-dir", type=Path)
    arg_parser.add_argument("cargo_build_args", nargs="*")
    args = arg_parser.parse_args()
    cross: str = args.cross
    output: Optional[Path] = args.output
    source_dir: Path = args.source_dir or Path.cwd()
    target_dir: Optional[Path] = args.target_dir
    cargo_build_args: List[str] = args.cargo_build_args

    cargo_path = shutil.which("cargo")
    if cargo_path is None:
        raise RuntimeError("'cargo' executable not found in PATH. Do you have Rust installed?")

    if target_dir:
        target_dir = target_dir.resolve()

    with cwd(source_dir):
        all_args = [cargo_path]

        if "windows-msvc" in cross:
            all_args.extend(("xwin", "build"))
        elif cross:
            all_args.append("zigbuild")
        else:
            all_args.append("build")

        if target_dir:
            all_args.extend(("--target-dir", str(target_dir)))

        all_args.extend(cargo_build_args)
        if cross:
            all_args.extend(("--target", cross))

        print("+", "cargo", *all_args[1:], file=sys.stderr)
        subprocess.run(all_args, check=True)

    if output is not None:
        # Find the requested output file
        base_dir = target_dir or (source_dir / "target")
        cross_dir = cross.partition(".")[0] if cross else ""
        search_dir = base_dir / cross_dir / cargo_build_type(cargo_build_args)

        # Allow an alternative match on missing/present "lib". Usually meson expects
        # "libZZZ.so", but Rust produced "ZZZ.so", or vice versa.
        names = [output.name]
        if output.name.startswith("lib"):
            names.append(output.name[3:])
        else:
            names.append("lib" + output.name)

        # Copy out the file
        for name in names:
            file = search_dir / name
            if file.exists():
                shutil.copyfile(file, output)
                break
        else:
            raise FileNotFoundError(
                f"Expected Rust to produce {' or '.join(names)} in {search_dir}, "
                "but none were found"
            )
