from typing import Type

from impuls.model import Calendar, Date

from .template_entity import AbstractTestEntity


class TestCalendar(AbstractTestEntity.Template[Calendar]):
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
            extra_fields_json=None,
        )

    def get_type(self) -> Type[Calendar]:
        return Calendar

    def test_sql_marshall(self) -> None:
        self.assertTupleEqual(
            self.get_entity().sql_marshall(),
            ("0", 1, 1, 1, 1, 1, 0, 0, "2020-01-01", "2020-03-31", "Workdays", None),
        )

    def test_sql_primary_key(self) -> None:
        self.assertTupleEqual(self.get_entity().sql_primary_key(), ("0",))

    def test_sql_unmarshall(self) -> None:
        c = Calendar.sql_unmarshall(
            ("0", 1, 1, 1, 1, 1, 0, 0, "2020-01-01", "2020-03-31", "Workdays", None),
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
        self.assertIsNone(c.extra_fields_json)

    def test_compressed_weekdays(self) -> None:
        self.assertEqual(self.get_entity().compressed_weekdays, 0b001_1111)

    def test_compute_active_dates(self) -> None:
        c = Calendar(
            id="0",
            monday=True,
            tuesday=True,
            wednesday=True,
            thursday=True,
            friday=False,
            saturday=False,
            sunday=False,
            start_date=Date(2020, 1, 1),
            end_date=Date(2020, 1, 11),
        )
        self.assertSetEqual(
            c.compute_active_dates(),
            {
                Date(2020, 1, 1),
                Date(2020, 1, 2),
                Date(2020, 1, 6),
                Date(2020, 1, 7),
                Date(2020, 1, 8),
                Date(2020, 1, 9),
            },
        )
