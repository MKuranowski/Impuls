# © Copyright 2022-2024 Mikołaj Kuranowski
# SPDX-License-Identifier: GPL-3.0-or-later

import concurrent.futures
import logging
import multiprocessing
import shutil
import subprocess
import sys
from argparse import ArgumentParser
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import NamedTuple

from impuls import initialize_logging

MESON_CROSS_FILES_DIR = Path(__file__).with_name("cross")  # NOTE: Absolute dir is necessary
WHEEL_PYTHON_TAG = "py3"
WHEEL_ABI_TAG = "none"


class Configuration(NamedTuple):
    meson_cross_file: Path
    wheel_platform_tag: str


# NOTE: The platform tag version requirements must be kept in sync with
#       platform/ABI version requirements in cross-compilation files.
#       E.g. wheel platform tag "macosx_11_0" requires zig target platform "macos.11.0",
#       wheel platform tag "manylinux_2_17" requires zig target abi "gnu.2.17".
#       Zig doesn't allow to specify target abi version for musl, so we just make up 1.1 support.
# NOTE: "manylinux2014" is a legacy alias for "manylinux_2_17".

CONFIGURATIONS = {
    "aarch64-linux-gnu": Configuration(
        MESON_CROSS_FILES_DIR / "aarch64-linux-gnu.ini",
        "manylinux2014_aarch64.manylinux_2_17_aarch64",
    ),
    "aarch64-linux-musl": Configuration(
        MESON_CROSS_FILES_DIR / "aarch64-linux-musl.ini",
        "musllinux_1_1_aarch64",
    ),
    "aarch64-macos": Configuration(
        MESON_CROSS_FILES_DIR / "aarch64-macos.ini",
        "macosx_11_0_arm64",
    ),
    "aarch64-windows": Configuration(
        MESON_CROSS_FILES_DIR / "aarch64-windows.ini",
        "win_arm64",
    ),
    # riscv64-linux-gnu is broken due to https://github.com/ziglang/zig/issues/3340
    # "riscv64-linux-gnu": Configuration(
    #     MESON_CROSS_FILES_DIR / "riscv64-linux-gnu.ini",
    #     "manylinux2014_riscv64.manylinux_2_17_riscv64",
    # ),
    # riscv64-linux-musl is rejected by PyPI with 400 Bad Request due to 'musllinux_1_1_riscv64'
    # being an unsupported platform tag :^(
    # "riscv64-linux-musl": Configuration(
    #     MESON_CROSS_FILES_DIR / "riscv64-linux-musl.ini",
    #     "musllinux_1_1_riscv64",
    # ),
    "x86_64-linux-gnu": Configuration(
        MESON_CROSS_FILES_DIR / "x86_64-linux-gnu.ini",
        "manylinux2014_x86_64.manylinux_2_17_x86_64",
    ),
    "x86_64-linux-musl": Configuration(
        MESON_CROSS_FILES_DIR / "x86_64-linux-musl.ini",
        "musllinux_1_1_x86_64",
    ),
    "x86_64-macos": Configuration(
        MESON_CROSS_FILES_DIR / "x86_64-macos.ini",
        "macosx_11_0_x86_64",
    ),
    "x86_64-windows": Configuration(
        MESON_CROSS_FILES_DIR / "x86_64-windows.ini",
        "win_amd64",
    ),
}


@dataclass
class Builder:
    logger: logging.Logger
    name: str
    config: Configuration
    verbose: bool = False

    def find_wheel_in(self, dir: Path) -> Path:
        wheels = list(dir.glob("*.whl"))
        if len(wheels) != 1:
            raise ValueError(
                f'{len(wheels)} wheels were created by "python -m build", expected exactly 1',
            )
        return wheels[0]

    def compile(self, target_dir: Path) -> Path:
        self.logger.info("Starting compilation")
        pipe = None if self.verbose else subprocess.DEVNULL
        subprocess.run(
            [
                "python3",
                "-m",
                "build",
                "--wheel",
                "--outdir",
                str(target_dir),
                "-C",
                f"setup-args=--cross-file={self.config.meson_cross_file}",
                # "-D buildtype=release" is automatically added by meson-python
            ],
            stdin=pipe,
            stdout=pipe,
            stderr=pipe,
            check=True,
        )
        self.logger.info("Compilation completed")
        return self.find_wheel_in(target_dir)

    def rename_wheel(self, old_name: Path) -> Path:
        self.logger.info("Fixing wheel tags")
        default_pipe = None if self.verbose else subprocess.DEVNULL
        result = subprocess.run(
            [
                "python3",
                "-m",
                "wheel",
                "tags",
                "--python-tag",
                WHEEL_PYTHON_TAG,
                "--abi-tag",
                WHEEL_ABI_TAG,
                "--platform-tag",
                self.config.wheel_platform_tag,
                str(old_name),
            ],
            stdin=default_pipe,
            stdout=subprocess.PIPE,
            stderr=default_pipe,
            check=True,
            text=True,
        )
        output_lines = result.stdout.splitlines()
        output_filename = output_lines[-1]
        assert output_filename.endswith(".whl")
        return old_name.with_name(output_filename)

    def build(self, output_dir: Path) -> bool:
        with TemporaryDirectory(prefix="impuls-build-wheels") as temp_dir_name:
            temp_dir = Path(temp_dir_name)
            try:
                broken_wheel_path = self.compile(temp_dir)
                fixed_wheel_path = self.rename_wheel(broken_wheel_path)
                shutil.copy(fixed_wheel_path, output_dir / fixed_wheel_path.name)
                return True
            except Exception:
                self.logger.error("Build failed:", exc_info=True)
                return False

    @classmethod
    def create_and_build(
        cls,
        name: str,
        config: Configuration,
        output_dir: Path = Path("dist"),
        verbose: bool = False,
    ) -> bool:
        return cls(
            logger=logging.getLogger(f"builder.{name}"),
            name=name,
            config=config,
            verbose=verbose,
        ).build(output_dir)


def main() -> None:
    arg_parser = ArgumentParser()
    arg_parser.add_argument("-v", "--verbose", action="store_true", help="show build output")
    arg_parser.add_argument(
        "-o",
        "--outdir",
        type=Path,
        default="dist",
        help="output directory (defaults to dist)",
    )
    arg_parser.add_argument(
        "-j",
        "--jobs",
        type=parse_jobs,
        default="auto",
        help=(
            'how many builds to run in parallel (integer or "auto" to use all available CPUs)'
            " (defaults to auto)"
        ),
    )
    arg_parser.add_argument(
        "configurations",
        nargs="*",
        choices=CONFIGURATIONS.keys(),
        help="which wheels should be built? (defaults to all)",
    )
    args = arg_parser.parse_args()

    initialize_logging(verbose=False)
    args.outdir.mkdir(exist_ok=True)

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.jobs) as pool:
        # Submit builds of all configurations to the executor
        futures = [
            pool.submit(
                Builder.create_and_build,
                config_name,
                CONFIGURATIONS[config_name],
                args.outdir,
                args.verbose,
            )
            for config_name in (args.configurations or CONFIGURATIONS.keys())
        ]

        # Await for all builds to complete and exit indicating a failure if any of the
        # builds has failed
        exit_code = 0
        for future in futures:
            if not future.result():
                exit_code = 1

    sys.exit(exit_code)


def parse_jobs(x: str) -> int:
    if x.casefold() == "auto":
        return multiprocessing.cpu_count()
    return int(x)


if __name__ == "__main__":
    main()
