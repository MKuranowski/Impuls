from typing import Type

from impuls.model import CalendarException, Date

from .template_entity import AbstractTestEntity


class TestCalendarException(AbstractTestEntity.Template[CalendarException]):
    def get_entity(self) -> CalendarException:
        return CalendarException(
            calendar_id="0",
            date=Date(2020, 2, 29),
            exception_type=CalendarException.Type.ADDED,
        )

    def get_type(self) -> Type[CalendarException]:
        return CalendarException

    def test_sql_marshall(self) -> None:
        self.assertTupleEqual(self.get_entity().sql_marshall(), ("0", "2020-02-29", 1))

    def test_sql_primary_key(self) -> None:
        self.assertTupleEqual(self.get_entity().sql_primary_key(), ("0", "2020-02-29"))

    def test_sql_unmarshall(self) -> None:
        ce = CalendarException.sql_unmarshall(("0", "2020-02-29", 1))

        self.assertEqual(ce.calendar_id, "0")
        self.assertEqual(ce.date, Date(2020, 2, 29))
        self.assertEqual(ce.exception_type, CalendarException.Type.ADDED)

    def test_reflect_in_active_dates(self) -> None:
        dates = {
            Date(2020, 1, 1),
            Date(2020, 1, 2),
            Date(2020, 1, 6),
            Date(2020, 1, 7),
            Date(2020, 1, 8),
            Date(2020, 1, 9),
        }
        exceptions = [
            CalendarException("0", Date(2020, 1, 10), CalendarException.Type.ADDED),
            CalendarException("0", Date(2020, 1, 1), CalendarException.Type.REMOVED),
        ]

        returned_dates = CalendarException.reflect_in_active_dates(dates, exceptions)
        self.assertIs(dates, returned_dates)
        self.assertSetEqual(
            dates,
            {
                Date(2020, 1, 2),
                Date(2020, 1, 6),
                Date(2020, 1, 7),
                Date(2020, 1, 8),
                Date(2020, 1, 9),
                Date(2020, 1, 10),
            },
        )
