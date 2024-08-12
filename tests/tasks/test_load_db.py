# © Copyright 2024 Mikołaj Kuranowski
# SPDX-License-Identifier: GPL-3.0-or-later

from impuls.model import Agency, StopTime
from impuls.resource import LocalResource
from impuls.tasks import LoadDB

from .template_testcase import FIXTURES_DIR, AbstractTestTask


class TestLoadDb(AbstractTestTask.Template):
    db_name = None
    resources = {"wkd.db": LocalResource(FIXTURES_DIR / "wkd.db")}

    def test(self) -> None:
        self.assertEqual(self.runtime.db.count(Agency), 0)
        self.assertEqual(self.runtime.db.count(StopTime), 0)

        task = LoadDB("wkd.db")
        task.execute(self.runtime)

        self.assertEqual(self.runtime.db.count(Agency), 1)
        self.assertEqual(self.runtime.db.count(StopTime), 6276)
