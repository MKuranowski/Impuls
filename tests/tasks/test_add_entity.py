from impuls.model import Agency
from impuls.tasks import AddEntity

from .template_testcase import AbstractTestTask


class TestAddEntity(AbstractTestTask.Template):
    def test(self) -> None:
        task = AddEntity(
            Agency(
                id="1",
                name="New Agency",
                url="https://example.com",
                timezone="Europe/Warsaw",
            )
        )
        task.execute(self.runtime)

        agencies = list(self.runtime.db.retrieve_all(Agency))
        self.assertEqual(len(agencies), 2)

        added_agency = agencies[1]
        self.assertEqual(added_agency.id, "1")
        self.assertEqual(added_agency.name, "New Agency")
        self.assertEqual(added_agency.url, "https://example.com")
        self.assertEqual(added_agency.timezone, "Europe/Warsaw")
