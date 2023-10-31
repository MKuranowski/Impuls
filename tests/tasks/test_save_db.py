from impuls.db import DBConnection
from impuls.model import Agency, StopTime
from impuls.tasks import SaveDB
from impuls.tools.testing_mocks import MockFile

from .template_testcase import AbstractTestTask


class TestSaveDB(AbstractTestTask.Template):
    def test(self) -> None:
        with MockFile() as tempfile:
            task = SaveDB(tempfile)
            task.execute(self.runtime)

            self.assertTrue(tempfile.exists())

            with DBConnection(tempfile) as saved_db:
                self.assertEqual(saved_db.count(Agency), 1)
                self.assertEqual(saved_db.count(StopTime), 6276)
