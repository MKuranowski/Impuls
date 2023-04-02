from typing import Type, final

from impuls.model import Calendar, Date

from .base_entity_test_case import BaseEntity


@final
class TestCalendar(BaseEntity.TestCase[Calendar]):
    def get_entity(self) -> Calendar:
        return Calendar(
            id="0",
            monday=True,
            tuesday=True,
            wednesday=True,
            thursday=True,
            friday=True,
            saturday=False,
            sunday=False,
            start_date=Date(2020, 1, 1),
            end_date=Date(2020, 3, 31),
            desc="Workdays",
        )

    def get_type(self) -> Type[Calendar]:
        return Calendar

    def test_gtfs_marshall(self) -> None:
        self.assertDictEqual(
            self.get_entity().gtfs_marshall(),
            {
                "service_id": "0",
                "monday": "1",
                "tuesday": "1",
                "wednesday": "1",
                "thursday": "1",
                "friday": "1",
                "saturday": "0",
                "sunday": "0",
                "start_date": "20200101",
                "end_date": "20200331",
                "service_desc": "Workdays",
            },
        )

    def test_gtfs_unmarshall(self) -> None:
        c = Calendar.gtfs_unmarshall(
            {
                "service_id": "0",
                "monday": "1",
                "tuesday": "1",
                "wednesday": "1",
                "thursday": "1",
                "friday": "1",
                "saturday": "0",
                "sunday": "0",
                "start_date": "20200101",
                "end_date": "20200331",
                "service_desc": "Workdays",
            }
        )

        self.assertEqual(c.id, "0")
        self.assertEqual(c.monday, True)
        self.assertEqual(c.tuesday, True)
        self.assertEqual(c.wednesday, True)
        self.assertEqual(c.thursday, True)
        self.assertEqual(c.friday, True)
        self.assertEqual(c.saturday, False)
        self.assertEqual(c.sunday, False)
        self.assertEqual(c.start_date, Date(2020, 1, 1))
        self.assertEqual(c.end_date, Date(2020, 3, 31))
        self.assertEqual(c.desc, "Workdays")

    def test_sql_marshall(self) -> None:
        self.assertTupleEqual(
            self.get_entity().sql_marshall(),
            ("0", 1, 1, 1, 1, 1, 0, 0, "2020-01-01", "2020-03-31", "Workdays"),
        )

    def test_sql_primary_key(self) -> None:
        self.assertTupleEqual(self.get_entity().sql_primary_key(), ("0",))

    def test_sql_unmarshall(self) -> None:
        c = Calendar.sql_unmarshall(
            ("0", 1, 1, 1, 1, 1, 0, 0, "2020-01-01", "2020-03-31", "Workdays"),
        )

        self.assertEqual(c.id, "0")
        self.assertEqual(c.monday, True)
        self.assertEqual(c.tuesday, True)
        self.assertEqual(c.wednesday, True)
        self.assertEqual(c.thursday, True)
        self.assertEqual(c.friday, True)
        self.assertEqual(c.saturday, False)
        self.assertEqual(c.sunday, False)
        self.assertEqual(c.start_date, Date(2020, 1, 1))
        self.assertEqual(c.end_date, Date(2020, 3, 31))
        self.assertEqual(c.desc, "Workdays")
