from impuls.db import DBConnection
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
                self.assertEqual(
                    saved_db.raw_execute("SELECT COUNT(*) FROM agencies").one_must("count")[0],
                    1,
                )
                self.assertEqual(
                    saved_db.raw_execute("SELECT COUNT(*) FROM stop_times").one_must("count")[0],
                    6276,
                )
