# © Copyright 2022-2024 Mikołaj Kuranowski
# SPDX-License-Identifier: GPL-3.0-or-later

import shutil
from pathlib import Path

from generate_db_from_gtfs import generate_db_from_gtfs
from impuls import initialize_logging


def main() -> None:
    initialize_logging(verbose=True)
    fixture_names = ["wkd", "wkd-next"]

    for fixture_name in fixture_names:
        gtfs_path = Path("tests/tasks/fixtures/", f"{fixture_name}.zip")
        db_path = Path("tests/tasks/fixtures/", f"{fixture_name}.db")
        generate_db_from_gtfs(gtfs_path, db_path)

    shutil.copy("tests/tasks/fixtures/wkd.db", "tests/fixtures/wkd.db")


if __name__ == "__main__":
    main()
