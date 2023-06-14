from typing import Type, final

from impuls.model import CalendarException, Date

from .template_entity import AbstractTestEntity


@final
class TestCalendarException(AbstractTestEntity.Template[CalendarException]):
    def get_entity(self) -> CalendarException:
        return CalendarException(
            calendar_id="0",
            date=Date(2020, 2, 29),
            exception_type=CalendarException.Type.ADDED,
        )

    def get_type(self) -> Type[CalendarException]:
        return CalendarException

    def test_gtfs_marshall(self) -> None:
        self.assertDictEqual(
            self.get_entity().gtfs_marshall(),
            {
                "service_id": "0",
                "date": "20200229",
                "exception_type": "1",
            },
        )

    def test_gtfs_unmarshall(self) -> None:
        ce = CalendarException.gtfs_unmarshall(
            {
                "service_id": "0",
                "date": "20200229",
                "exception_type": "1",
            },
        )

        self.assertEqual(ce.calendar_id, "0")
        self.assertEqual(ce.date, Date(2020, 2, 29))
        self.assertEqual(ce.exception_type, CalendarException.Type.ADDED)

    def test_sql_marshall(self) -> None:
        self.assertTupleEqual(self.get_entity().sql_marshall(), ("0", "2020-02-29", 1))

    def test_sql_primary_key(self) -> None:
        self.assertTupleEqual(self.get_entity().sql_primary_key(), ("0", "2020-02-29"))

    def test_sql_unmarshall(self) -> None:
        ce = CalendarException.sql_unmarshall(("0", "2020-02-29", 1))

        self.assertEqual(ce.calendar_id, "0")
        self.assertEqual(ce.date, Date(2020, 2, 29))
        self.assertEqual(ce.exception_type, CalendarException.Type.ADDED)
