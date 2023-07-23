from impuls.model import Agency
from impuls.tasks import ExecuteSQL

from .template_testcase import AbstractTestTask


class TestExecuteSQL(AbstractTestTask.Template):
    def test(self) -> None:
        task = ExecuteSQL(
            task_name="add_agency",
            statement=(
                "INSERT INTO agencies (agency_id, name, url, timezone) "
                "VALUES ('1', 'New Agency', 'https://example.com', 'Europe/Warsaw')"
            ),
        )
        task.execute(self.runtime)

        agencies = list(self.runtime.db.retrieve_all(Agency))
        self.assertEqual(len(agencies), 2)

        added_agency = agencies[1]
        self.assertEqual(added_agency.id, "1")
        self.assertEqual(added_agency.name, "New Agency")
        self.assertEqual(added_agency.url, "https://example.com")
        self.assertEqual(added_agency.timezone, "Europe/Warsaw")
