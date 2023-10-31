from pathlib import Path

from impuls import LocalResource
from impuls.model import Agency, Calendar, CalendarException, Route, Stop, StopTime, Trip
from impuls.tasks import LoadBusManMDB

from .template_testcase import AbstractTestTask

FIXTURES = Path(__file__).with_name("fixtures")


class TestLoadBusmanMDB(AbstractTestTask.Template):
    db_name = None
    resources = {
        "wkd.mdb": LocalResource(FIXTURES / "wkd.mdb"),
    }

    def test(self) -> None:
        self.runtime.db.create(
            Agency("0", "Warszawska Kolej Dojazdowa", "https://wkd.com.pl", "Europe/Warsaw"),
        )
        t = LoadBusManMDB("wkd.mdb", "0")
        t.execute(self.runtime)

        self.assertEqual(self.runtime.db.count(Route), 3)
        self.assertEqual(self.runtime.db.count(Stop), 28)
        self.assertEqual(self.runtime.db.count(Calendar), 2)
        self.assertEqual(self.runtime.db.count(CalendarException), 0)
        self.assertEqual(self.runtime.db.count(Trip), 372)
        self.assertEqual(self.runtime.db.count(StopTime), 6276)
