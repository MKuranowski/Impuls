from math import inf
from unittest import TestCase

from impuls.model import Date
from impuls.tools.iteration import limit
from impuls.tools.temporal import (
    BoundedDateRange,
    EmptyDateRange,
    InfiniteDateRange,
    LeftUnboundedDateRange,
    RightUnboundedDateRange,
)


class TestEmptyDateRange(TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.r = EmptyDateRange()

    def test_compressed_weekdays(self) -> None:
        self.assertEqual(self.r.compressed_weekdays, 0)

    def test_contains(self) -> None:
        self.assertNotIn(Date(2020, 1, 1), self.r)
        self.assertNotIn(Date(2023, 1, 1), self.r)

    def test_len(self) -> None:
        self.assertEqual(self.r.len(), 0)

    def test_iter(self) -> None:
        self.assertListEqual(list(self.r), [])

    def test_eq(self) -> None:
        self.assertEqual(self.r, EmptyDateRange())
        self.assertNotEqual(self.r, InfiniteDateRange())

    def test_isdisjoint(self):
        self.assertTrue(self.r.isdisjoint(EmptyDateRange()))
        self.assertTrue(self.r.isdisjoint(InfiniteDateRange()))
        self.assertTrue(self.r.isdisjoint(LeftUnboundedDateRange(end=Date(2020, 3, 1))))
        self.assertTrue(self.r.isdisjoint(RightUnboundedDateRange(start=Date(2020, 1, 1))))
        self.assertTrue(self.r.isdisjoint(BoundedDateRange(Date(2020, 1, 1), Date(2020, 3, 1))))

    def test_issubset(self):
        self.assertTrue(self.r.issubset(EmptyDateRange()))
        self.assertTrue(self.r.issubset(InfiniteDateRange()))
        self.assertTrue(self.r.issubset(LeftUnboundedDateRange(end=Date(2020, 3, 1))))
        self.assertTrue(self.r.issubset(RightUnboundedDateRange(start=Date(2020, 1, 1))))
        self.assertTrue(self.r.issubset(BoundedDateRange(Date(2020, 1, 1), Date(2020, 3, 1))))

    def test_union(self):
        others = [
            EmptyDateRange(),
            InfiniteDateRange(),
            LeftUnboundedDateRange(end=Date(2020, 3, 1)),
            RightUnboundedDateRange(start=Date(2020, 1, 1)),
            BoundedDateRange(Date(2020, 1, 1), Date(2020, 3, 1)),
        ]
        for other in others:
            self.assertEqual(self.r.union(other), other)

    def test_intersection(self):
        others = [
            EmptyDateRange(),
            InfiniteDateRange(),
            LeftUnboundedDateRange(end=Date(2020, 3, 1)),
            RightUnboundedDateRange(start=Date(2020, 1, 1)),
            BoundedDateRange(Date(2020, 1, 1), Date(2020, 3, 1)),
        ]
        for other in others:
            self.assertEqual(self.r.intersection(other), self.r)

    def test_difference(self):
        others = [
            EmptyDateRange(),
            InfiniteDateRange(),
            LeftUnboundedDateRange(end=Date(2020, 3, 1)),
            RightUnboundedDateRange(start=Date(2020, 1, 1)),
            BoundedDateRange(Date(2020, 1, 1), Date(2020, 3, 1)),
        ]
        for other in others:
            self.assertEqual(self.r.intersection(other), self.r)


class TestInfiniteDateRange(TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.r = InfiniteDateRange()

    def test_compressed_weekdays(self) -> None:
        self.assertEqual(self.r.compressed_weekdays, 0b111_1111)

    def test_contains(self) -> None:
        self.assertIn(Date(2020, 1, 1), self.r)
        self.assertIn(Date(2023, 1, 1), self.r)

    def test_len(self) -> None:
        self.assertEqual(self.r.len(), inf)

    def test_iter(self) -> None:
        with self.assertRaises(RuntimeError):
            list(self.r)

    def test_eq(self) -> None:
        self.assertEqual(self.r, InfiniteDateRange())
        self.assertNotEqual(self.r, EmptyDateRange())

    def test_isdisjoint(self):
        self.assertTrue(self.r.isdisjoint(EmptyDateRange()))
        self.assertFalse(self.r.isdisjoint(InfiniteDateRange()))
        self.assertFalse(self.r.isdisjoint(LeftUnboundedDateRange(end=Date(2020, 3, 1))))
        self.assertFalse(self.r.isdisjoint(RightUnboundedDateRange(start=Date(2020, 1, 1))))
        self.assertFalse(self.r.isdisjoint(BoundedDateRange(Date(2020, 1, 1), Date(2020, 3, 1))))

    def test_issubset(self):
        self.assertTrue(self.r.issubset(InfiniteDateRange()))
        self.assertFalse(self.r.issubset(EmptyDateRange()))
        self.assertFalse(self.r.issubset(LeftUnboundedDateRange(end=Date(2020, 3, 1))))
        self.assertFalse(self.r.issubset(RightUnboundedDateRange(start=Date(2020, 1, 1))))
        self.assertFalse(self.r.issubset(BoundedDateRange(Date(2020, 1, 1), Date(2020, 3, 1))))

    def test_union(self):
        others = [
            EmptyDateRange(),
            InfiniteDateRange(),
            LeftUnboundedDateRange(end=Date(2020, 3, 1)),
            RightUnboundedDateRange(start=Date(2020, 1, 1)),
            BoundedDateRange(Date(2020, 1, 1), Date(2020, 3, 1)),
        ]
        for other in others:
            self.assertEqual(self.r.union(other), self.r)

    def test_intersection(self):
        others = [
            EmptyDateRange(),
            InfiniteDateRange(),
            LeftUnboundedDateRange(end=Date(2020, 3, 1)),
            RightUnboundedDateRange(start=Date(2020, 1, 1)),
            BoundedDateRange(Date(2020, 1, 1), Date(2020, 3, 1)),
        ]
        for other in others:
            self.assertEqual(self.r.intersection(other), other)

    def test_difference(self):
        self.assertEqual(self.r.difference(EmptyDateRange()), InfiniteDateRange())
        self.assertEqual(self.r.difference(InfiniteDateRange()), EmptyDateRange())
        self.assertEqual(
            self.r.difference(LeftUnboundedDateRange(end=Date(2020, 1, 1))),
            RightUnboundedDateRange(start=Date(2020, 1, 2)),
        )
        self.assertEqual(
            self.r.difference(RightUnboundedDateRange(start=Date(2020, 1, 1))),
            LeftUnboundedDateRange(end=Date(2019, 12, 31)),
        )
        with self.assertRaises(ArithmeticError):
            self.r.difference(BoundedDateRange(Date(2023, 1, 1), Date(2023, 3, 1)))


class TestLeftUnboundedDateRange(TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.r = LeftUnboundedDateRange(end=Date(2020, 3, 1))

    def test_compressed_weekdays(self) -> None:
        self.assertEqual(self.r.compressed_weekdays, 0b111_1111)

    def test_contains(self) -> None:
        self.assertIn(Date(2020, 1, 1), self.r)
        self.assertIn(Date(2020, 2, 29), self.r)
        self.assertIn(Date(2020, 3, 1), self.r)
        self.assertNotIn(Date(2020, 3, 2), self.r)
        self.assertNotIn(Date(2023, 1, 1), self.r)

    def test_len(self) -> None:
        self.assertEqual(self.r.len(), inf)

    def test_iter(self) -> None:
        self.assertListEqual(
            list(limit(self.r, 10)),
            [
                Date(2020, 3, 1),
                Date(2020, 2, 29),
                Date(2020, 2, 28),
                Date(2020, 2, 27),
                Date(2020, 2, 26),
                Date(2020, 2, 25),
                Date(2020, 2, 24),
                Date(2020, 2, 23),
                Date(2020, 2, 22),
                Date(2020, 2, 21),
            ],
        )

    def test_eq(self) -> None:
        self.assertNotEqual(self.r, InfiniteDateRange())
        self.assertNotEqual(self.r, EmptyDateRange())
        self.assertNotEqual(self.r, LeftUnboundedDateRange(end=Date(2022, 12, 31)))
        self.assertEqual(self.r, LeftUnboundedDateRange(end=Date(2020, 3, 1)))
        self.assertNotEqual(self.r, RightUnboundedDateRange(start=Date(2022, 12, 31)))

    def test_isdisjoint(self):
        self.assertTrue(self.r.isdisjoint(EmptyDateRange()))
        self.assertFalse(self.r.isdisjoint(InfiniteDateRange()))
        self.assertFalse(self.r.isdisjoint(LeftUnboundedDateRange(end=Date(2020, 3, 1))))

        self.assertFalse(self.r.isdisjoint(RightUnboundedDateRange(start=Date(2020, 1, 1))))
        self.assertFalse(self.r.isdisjoint(RightUnboundedDateRange(start=Date(2020, 3, 1))))
        self.assertTrue(self.r.isdisjoint(RightUnboundedDateRange(start=Date(2020, 3, 2))))

        self.assertFalse(self.r.isdisjoint(BoundedDateRange(Date(2012, 1, 1), Date(2012, 3, 1))))
        self.assertFalse(self.r.isdisjoint(BoundedDateRange(Date(2020, 1, 1), Date(2020, 3, 1))))
        self.assertFalse(self.r.isdisjoint(BoundedDateRange(Date(2020, 1, 1), Date(2020, 5, 1))))
        self.assertFalse(self.r.isdisjoint(BoundedDateRange(Date(2020, 3, 1), Date(2020, 3, 1))))
        self.assertTrue(self.r.isdisjoint(BoundedDateRange(Date(2020, 3, 2), Date(2020, 3, 2))))
        self.assertTrue(self.r.isdisjoint(BoundedDateRange(Date(2023, 1, 1), Date(2023, 3, 1))))

    def test_issubset(self):
        self.assertTrue(self.r.issubset(InfiniteDateRange()))
        self.assertFalse(self.r.issubset(EmptyDateRange()))

        self.assertFalse(self.r.issubset(LeftUnboundedDateRange(end=Date(2020, 1, 1))))
        self.assertFalse(self.r.issubset(LeftUnboundedDateRange(end=Date(2020, 2, 29))))
        self.assertTrue(self.r.issubset(LeftUnboundedDateRange(end=Date(2020, 3, 1))))
        self.assertTrue(self.r.issubset(LeftUnboundedDateRange(end=Date(2020, 3, 2))))
        self.assertTrue(self.r.issubset(LeftUnboundedDateRange(end=Date(2020, 5, 1))))

        self.assertFalse(self.r.issubset(RightUnboundedDateRange(start=Date(2020, 1, 1))))

        self.assertFalse(self.r.issubset(BoundedDateRange(Date(2020, 1, 1), Date(2020, 3, 1))))

    def test_union(self):
        self.assertEqual(self.r | EmptyDateRange(), self.r)
        self.assertEqual(self.r | InfiniteDateRange(), InfiniteDateRange())

        self.assertEqual(self.r | LeftUnboundedDateRange(end=Date(2020, 1, 1)), self.r)
        self.assertEqual(self.r | LeftUnboundedDateRange(end=Date(2020, 3, 1)), self.r)
        self.assertEqual(
            self.r | LeftUnboundedDateRange(end=Date(2020, 5, 1)),
            LeftUnboundedDateRange(end=Date(2020, 5, 1)),
        )

        self.assertEqual(
            self.r | RightUnboundedDateRange(start=Date(2020, 1, 1)),
            InfiniteDateRange(),
        )
        self.assertEqual(
            self.r | RightUnboundedDateRange(start=Date(2020, 2, 29)),
            InfiniteDateRange(),
        )
        self.assertEqual(
            self.r | RightUnboundedDateRange(start=Date(2020, 3, 1)),
            InfiniteDateRange(),
        )
        self.assertEqual(
            self.r | RightUnboundedDateRange(start=Date(2020, 3, 2)),
            InfiniteDateRange(),
        )
        with self.assertRaises(ArithmeticError):
            self.r.union(RightUnboundedDateRange(start=Date(2020, 3, 3)))
        with self.assertRaises(ArithmeticError):
            self.r.union(RightUnboundedDateRange(start=Date(2020, 5, 1)))

        self.assertEqual(
            self.r | BoundedDateRange(start=Date(2019, 12, 1), end=Date(2020, 1, 1)),
            LeftUnboundedDateRange(end=Date(2020, 3, 1)),
        )
        self.assertEqual(
            self.r | BoundedDateRange(start=Date(2020, 1, 1), end=Date(2020, 2, 29)),
            LeftUnboundedDateRange(end=Date(2020, 3, 1)),
        )
        self.assertEqual(
            self.r | BoundedDateRange(start=Date(2020, 3, 1), end=Date(2020, 3, 1)),
            LeftUnboundedDateRange(end=Date(2020, 3, 1)),
        )
        self.assertEqual(
            self.r | BoundedDateRange(start=Date(2020, 3, 2), end=Date(2020, 3, 2)),
            LeftUnboundedDateRange(end=Date(2020, 3, 2)),
        )
        with self.assertRaises(ArithmeticError):
            self.r.union(BoundedDateRange(start=Date(2020, 3, 3), end=Date(2020, 3, 3)))
        with self.assertRaises(ArithmeticError):
            self.r.union(BoundedDateRange(start=Date(2023, 1, 1), end=Date(2023, 5, 1)))

    def test_intersection(self):
        self.assertEqual(self.r & EmptyDateRange(), EmptyDateRange())
        self.assertEqual(self.r & InfiniteDateRange(), self.r)

        self.assertEqual(
            self.r & LeftUnboundedDateRange(end=Date(2020, 1, 1)),
            LeftUnboundedDateRange(end=Date(2020, 1, 1)),
        )
        self.assertEqual(self.r & LeftUnboundedDateRange(end=Date(2020, 3, 1)), self.r)
        self.assertEqual(self.r & LeftUnboundedDateRange(end=Date(2020, 5, 1)), self.r)

        self.assertEqual(
            self.r & RightUnboundedDateRange(start=Date(2020, 5, 1)),
            EmptyDateRange(),
        )
        self.assertEqual(
            self.r & RightUnboundedDateRange(start=Date(2020, 3, 2)),
            EmptyDateRange(),
        )
        self.assertEqual(
            self.r & RightUnboundedDateRange(start=Date(2020, 3, 1)),
            BoundedDateRange(Date(2020, 3, 1), Date(2020, 3, 1)),
        )
        self.assertEqual(
            self.r & RightUnboundedDateRange(start=Date(2020, 2, 29)),
            BoundedDateRange(Date(2020, 2, 29), Date(2020, 3, 1)),
        )
        self.assertEqual(
            self.r & RightUnboundedDateRange(start=Date(2020, 1, 1)),
            BoundedDateRange(Date(2020, 1, 1), Date(2020, 3, 1)),
        )

        self.assertEqual(
            self.r & BoundedDateRange(start=Date(2020, 5, 1), end=Date(2020, 5, 31)),
            EmptyDateRange(),
        )
        self.assertEqual(
            self.r & BoundedDateRange(start=Date(2020, 3, 2), end=Date(2020, 3, 31)),
            EmptyDateRange(),
        )
        self.assertEqual(
            self.r & BoundedDateRange(start=Date(2020, 3, 1), end=Date(2020, 3, 31)),
            BoundedDateRange(Date(2020, 3, 1), Date(2020, 3, 1)),
        )
        self.assertEqual(
            self.r & BoundedDateRange(start=Date(2020, 2, 29), end=Date(2020, 3, 1)),
            BoundedDateRange(Date(2020, 2, 29), Date(2020, 3, 1)),
        )
        self.assertEqual(
            self.r & BoundedDateRange(start=Date(2020, 1, 1), end=Date(2020, 1, 31)),
            BoundedDateRange(Date(2020, 1, 1), Date(2020, 1, 31)),
        )

    def test_difference(self):
        self.assertEqual(self.r - EmptyDateRange(), self.r)
        self.assertEqual(self.r - InfiniteDateRange(), EmptyDateRange())

        self.assertEqual(
            self.r - (LeftUnboundedDateRange(end=Date(2020, 1, 1))),
            BoundedDateRange(Date(2020, 1, 2), Date(2020, 3, 1)),
        )
        self.assertEqual(
            self.r - (LeftUnboundedDateRange(end=Date(2020, 2, 29))),
            BoundedDateRange(Date(2020, 3, 1), Date(2020, 3, 1)),
        )
        self.assertEqual(self.r - (LeftUnboundedDateRange(end=Date(2020, 3, 1))), EmptyDateRange())
        self.assertEqual(self.r - (LeftUnboundedDateRange(end=Date(2020, 5, 1))), EmptyDateRange())

        self.assertEqual(self.r - RightUnboundedDateRange(start=Date(2020, 5, 1)), self.r)
        self.assertEqual(self.r - RightUnboundedDateRange(start=Date(2020, 3, 2)), self.r)
        self.assertEqual(
            self.r - RightUnboundedDateRange(start=Date(2020, 3, 1)),
            LeftUnboundedDateRange(end=Date(2020, 2, 29)),
        )
        self.assertEqual(
            self.r - RightUnboundedDateRange(start=Date(2020, 1, 1)),
            LeftUnboundedDateRange(end=Date(2019, 12, 31)),
        )

        self.assertEqual(
            self.r - BoundedDateRange(Date(2020, 1, 1), Date(2020, 5, 1)),
            LeftUnboundedDateRange(end=Date(2019, 12, 31)),
        )
        self.assertEqual(
            self.r - BoundedDateRange(Date(2020, 2, 29), Date(2020, 5, 1)),
            LeftUnboundedDateRange(end=Date(2020, 2, 28)),
        )
        self.assertEqual(
            self.r - BoundedDateRange(Date(2020, 3, 1), Date(2020, 3, 1)),
            LeftUnboundedDateRange(end=Date(2020, 2, 29)),
        )
        self.assertEqual(self.r - BoundedDateRange(Date(2020, 3, 2), Date(2020, 3, 2)), self.r)
        with self.assertRaises(ArithmeticError):
            self.r.difference(BoundedDateRange(Date(2020, 1, 1), Date(2020, 2, 1)))
        with self.assertRaises(ArithmeticError):
            self.r.difference(BoundedDateRange(Date(2020, 1, 1), Date(2020, 2, 29)))


class TestRightUnboundedDateRange(TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.r = RightUnboundedDateRange(start=Date(2020, 3, 1))

    def test_compressed_weekdays(self) -> None:
        self.assertEqual(self.r.compressed_weekdays, 0b111_1111)

    def test_contains(self) -> None:
        self.assertNotIn(Date(2020, 1, 1), self.r)
        self.assertNotIn(Date(2020, 2, 29), self.r)
        self.assertIn(Date(2020, 3, 1), self.r)
        self.assertIn(Date(2020, 3, 2), self.r)
        self.assertIn(Date(2023, 1, 1), self.r)

    def test_len(self) -> None:
        self.assertEqual(self.r.len(), inf)

    def test_iter(self) -> None:
        self.assertListEqual(
            list(limit(self.r, 10)),
            [
                Date(2020, 3, 1),
                Date(2020, 3, 2),
                Date(2020, 3, 3),
                Date(2020, 3, 4),
                Date(2020, 3, 5),
                Date(2020, 3, 6),
                Date(2020, 3, 7),
                Date(2020, 3, 8),
                Date(2020, 3, 9),
                Date(2020, 3, 10),
            ],
        )

    def test_eq(self) -> None:
        self.assertNotEqual(self.r, InfiniteDateRange())
        self.assertNotEqual(self.r, EmptyDateRange())
        self.assertNotEqual(self.r, RightUnboundedDateRange(start=Date(2022, 12, 31)))
        self.assertEqual(self.r, RightUnboundedDateRange(start=Date(2020, 3, 1)))
        self.assertNotEqual(self.r, LeftUnboundedDateRange(end=Date(2022, 12, 31)))

    def test_isdisjoint(self):
        self.assertTrue(self.r.isdisjoint(EmptyDateRange()))
        self.assertFalse(self.r.isdisjoint(InfiniteDateRange()))
        self.assertFalse(self.r.isdisjoint(RightUnboundedDateRange(start=Date(2020, 3, 1))))

        self.assertTrue(self.r.isdisjoint(LeftUnboundedDateRange(end=Date(2020, 1, 1))))
        self.assertFalse(self.r.isdisjoint(LeftUnboundedDateRange(end=Date(2020, 3, 1))))
        self.assertFalse(self.r.isdisjoint(LeftUnboundedDateRange(end=Date(2020, 3, 2))))

        self.assertTrue(self.r.isdisjoint(BoundedDateRange(Date(2012, 1, 1), Date(2012, 3, 1))))
        self.assertTrue(self.r.isdisjoint(BoundedDateRange(Date(2020, 1, 1), Date(2020, 2, 29))))
        self.assertFalse(self.r.isdisjoint(BoundedDateRange(Date(2020, 1, 1), Date(2020, 3, 1))))
        self.assertFalse(self.r.isdisjoint(BoundedDateRange(Date(2020, 1, 1), Date(2020, 5, 1))))
        self.assertFalse(self.r.isdisjoint(BoundedDateRange(Date(2020, 3, 1), Date(2020, 3, 1))))
        self.assertFalse(self.r.isdisjoint(BoundedDateRange(Date(2020, 3, 2), Date(2020, 3, 2))))
        self.assertFalse(self.r.isdisjoint(BoundedDateRange(Date(2023, 1, 1), Date(2023, 3, 1))))

    def test_issubset(self):
        self.assertTrue(self.r.issubset(InfiniteDateRange()))
        self.assertFalse(self.r.issubset(EmptyDateRange()))

        self.assertTrue(self.r.issubset(RightUnboundedDateRange(start=Date(2020, 1, 1))))
        self.assertTrue(self.r.issubset(RightUnboundedDateRange(start=Date(2020, 2, 29))))
        self.assertTrue(self.r.issubset(RightUnboundedDateRange(start=Date(2020, 3, 1))))
        self.assertFalse(self.r.issubset(RightUnboundedDateRange(start=Date(2020, 3, 2))))
        self.assertFalse(self.r.issubset(RightUnboundedDateRange(start=Date(2020, 5, 1))))

        self.assertFalse(self.r.issubset(LeftUnboundedDateRange(end=Date(2020, 5, 1))))

        self.assertFalse(self.r.issubset(BoundedDateRange(Date(2020, 1, 1), Date(2020, 3, 1))))

    def test_union(self):
        self.assertEqual(self.r | EmptyDateRange(), self.r)
        self.assertEqual(self.r | InfiniteDateRange(), InfiniteDateRange())

        self.assertEqual(
            self.r | RightUnboundedDateRange(start=Date(2020, 1, 1)),
            RightUnboundedDateRange(start=Date(2020, 1, 1)),
        )
        self.assertEqual(self.r | RightUnboundedDateRange(start=Date(2020, 3, 1)), self.r)
        self.assertEqual(self.r | RightUnboundedDateRange(start=Date(2020, 5, 1)), self.r)

        self.assertEqual(
            self.r | LeftUnboundedDateRange(end=Date(2020, 5, 1)),
            InfiniteDateRange(),
        )
        self.assertEqual(
            self.r | LeftUnboundedDateRange(end=Date(2020, 3, 2)),
            InfiniteDateRange(),
        )
        self.assertEqual(
            self.r | LeftUnboundedDateRange(end=Date(2020, 3, 1)),
            InfiniteDateRange(),
        )
        self.assertEqual(
            self.r | LeftUnboundedDateRange(end=Date(2020, 2, 29)),
            InfiniteDateRange(),
        )
        with self.assertRaises(ArithmeticError):
            self.r.union(LeftUnboundedDateRange(end=Date(2020, 2, 28)))
        with self.assertRaises(ArithmeticError):
            self.r.union(LeftUnboundedDateRange(end=Date(2020, 1, 1)))

        self.assertEqual(self.r | BoundedDateRange(Date(2020, 5, 1), Date(2020, 5, 31)), self.r)
        self.assertEqual(self.r | BoundedDateRange(Date(2020, 3, 1), Date(2020, 5, 31)), self.r)
        self.assertEqual(
            self.r | BoundedDateRange(Date(2020, 2, 1), Date(2020, 3, 1)),
            RightUnboundedDateRange(start=Date(2020, 2, 1)),
        )
        self.assertEqual(
            self.r | BoundedDateRange(Date(2020, 2, 1), Date(2020, 2, 29)),
            RightUnboundedDateRange(start=Date(2020, 2, 1)),
        )
        with self.assertRaises(ArithmeticError):
            self.r.union(BoundedDateRange(Date(2020, 2, 1), Date(2020, 2, 28)))
        with self.assertRaises(ArithmeticError):
            self.r.union(BoundedDateRange(Date(2020, 1, 1), Date(2020, 1, 31)))

    def test_intersection(self):
        self.assertEqual(self.r & EmptyDateRange(), EmptyDateRange())
        self.assertEqual(self.r & InfiniteDateRange(), self.r)

        self.assertEqual(
            self.r & RightUnboundedDateRange(start=Date(2020, 5, 1)),
            RightUnboundedDateRange(start=Date(2020, 5, 1)),
        )
        self.assertEqual(self.r & RightUnboundedDateRange(start=Date(2020, 3, 1)), self.r)
        self.assertEqual(self.r & RightUnboundedDateRange(start=Date(2020, 1, 1)), self.r)

        self.assertEqual(
            self.r & LeftUnboundedDateRange(end=Date(2020, 1, 1)),
            EmptyDateRange(),
        )
        self.assertEqual(
            self.r & LeftUnboundedDateRange(end=Date(2020, 2, 29)),
            EmptyDateRange(),
        )
        self.assertEqual(
            self.r & LeftUnboundedDateRange(end=Date(2020, 3, 1)),
            BoundedDateRange(Date(2020, 3, 1), Date(2020, 3, 1)),
        )
        self.assertEqual(
            self.r & LeftUnboundedDateRange(end=Date(2020, 3, 2)),
            BoundedDateRange(Date(2020, 3, 1), Date(2020, 3, 2)),
        )
        self.assertEqual(
            self.r & LeftUnboundedDateRange(end=Date(2020, 5, 1)),
            BoundedDateRange(Date(2020, 3, 1), Date(2020, 5, 1)),
        )

        self.assertEqual(
            self.r & BoundedDateRange(start=Date(2020, 1, 1), end=Date(2020, 1, 31)),
            EmptyDateRange(),
        )
        self.assertEqual(
            self.r & BoundedDateRange(start=Date(2020, 2, 1), end=Date(2020, 2, 29)),
            EmptyDateRange(),
        )
        self.assertEqual(
            self.r & BoundedDateRange(start=Date(2020, 2, 1), end=Date(2020, 3, 1)),
            BoundedDateRange(Date(2020, 3, 1), Date(2020, 3, 1)),
        )
        self.assertEqual(
            self.r & BoundedDateRange(start=Date(2020, 2, 1), end=Date(2020, 3, 31)),
            BoundedDateRange(Date(2020, 3, 1), Date(2020, 3, 31)),
        )
        self.assertEqual(
            self.r & BoundedDateRange(start=Date(2020, 5, 1), end=Date(2020, 5, 31)),
            BoundedDateRange(Date(2020, 5, 1), Date(2020, 5, 31)),
        )

    def test_difference(self):
        self.assertEqual(self.r - EmptyDateRange(), self.r)
        self.assertEqual(self.r - InfiniteDateRange(), EmptyDateRange())

        self.assertEqual(
            self.r - RightUnboundedDateRange(start=Date(2020, 5, 1)),
            BoundedDateRange(Date(2020, 3, 1), Date(2020, 4, 30)),
        )
        self.assertEqual(
            self.r - RightUnboundedDateRange(start=Date(2020, 3, 2)),
            BoundedDateRange(Date(2020, 3, 1), Date(2020, 3, 1)),
        )
        self.assertEqual(
            self.r - RightUnboundedDateRange(start=Date(2020, 3, 1)),
            EmptyDateRange(),
        )
        self.assertEqual(
            self.r - RightUnboundedDateRange(start=Date(2020, 1, 1)),
            EmptyDateRange(),
        )

        self.assertEqual(self.r - LeftUnboundedDateRange(end=Date(2020, 1, 1)), self.r)
        self.assertEqual(self.r - LeftUnboundedDateRange(end=Date(2020, 2, 29)), self.r)
        self.assertEqual(
            self.r - LeftUnboundedDateRange(end=Date(2020, 3, 1)),
            RightUnboundedDateRange(start=Date(2020, 3, 2)),
        )
        self.assertEqual(
            self.r - LeftUnboundedDateRange(end=Date(2020, 5, 1)),
            RightUnboundedDateRange(start=Date(2020, 5, 2)),
        )

        self.assertEqual(
            self.r - BoundedDateRange(Date(2020, 1, 1), Date(2020, 5, 1)),
            RightUnboundedDateRange(start=Date(2020, 5, 2)),
        )
        self.assertEqual(
            self.r - BoundedDateRange(Date(2020, 2, 29), Date(2020, 5, 1)),
            RightUnboundedDateRange(start=Date(2020, 5, 2)),
        )
        self.assertEqual(
            self.r - BoundedDateRange(Date(2020, 3, 1), Date(2020, 3, 1)),
            RightUnboundedDateRange(start=Date(2020, 3, 2)),
        )
        self.assertEqual(self.r - BoundedDateRange(Date(2020, 2, 29), Date(2020, 2, 29)), self.r)
        with self.assertRaises(ArithmeticError):
            self.r.difference(BoundedDateRange(Date(2020, 4, 1), Date(2020, 5, 1)))
        with self.assertRaises(ArithmeticError):
            self.r.difference(BoundedDateRange(Date(2020, 3, 2), Date(2020, 5, 1)))


class TestBoundedDateRange(TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.long = BoundedDateRange(Date(2020, 3, 1), Date(2020, 4, 30))
        self.short = BoundedDateRange(Date(2020, 3, 1), Date(2020, 3, 7))

    def test_compressed_weekdays(self) -> None:
        #      March 2020
        # Mo Tu We Th Fr Sa Su
        #                    1
        #  2  3  4  5  6  7  8
        #  9 10 11 12 13 14 15
        # 16 17 18 19 20 21 22
        # 23 24 25 26 27 28 29
        # 30 31

        self.assertEqual(self.long.compressed_weekdays, 0b111_1111)
        self.assertEqual(self.short.compressed_weekdays, 0b111_1111)
        self.assertEqual(
            BoundedDateRange(Date(2020, 3, 3), Date(2020, 3, 6)).compressed_weekdays,
            0b001_1110,
        )

    def test_contains(self) -> None:
        self.assertNotIn(Date(2020, 1, 1), self.long)
        self.assertNotIn(Date(2020, 2, 29), self.long)
        self.assertIn(Date(2020, 3, 1), self.long)
        self.assertIn(Date(2020, 4, 1), self.long)
        self.assertIn(Date(2020, 4, 30), self.long)
        self.assertNotIn(Date(2020, 5, 1), self.long)
        self.assertNotIn(Date(2020, 7, 1), self.long)

    def test_len(self) -> None:
        self.assertEqual(self.long.len(), 61)
        self.assertEqual(self.short.len(), 7)

    def test_iter(self) -> None:
        self.assertEqual(len(list(self.long)), 61)
        self.assertListEqual(
            list(self.short),
            [
                Date(2020, 3, 1),
                Date(2020, 3, 2),
                Date(2020, 3, 3),
                Date(2020, 3, 4),
                Date(2020, 3, 5),
                Date(2020, 3, 6),
                Date(2020, 3, 7),
            ],
        )

    def test_eq(self) -> None:
        self.assertEqual(self.long, BoundedDateRange(Date(2020, 3, 1), Date(2020, 4, 30)))
        self.assertEqual(self.short, BoundedDateRange(Date(2020, 3, 1), Date(2020, 3, 7)))
        self.assertNotEqual(self.long, self.short)

    def test_isdisjoint(self) -> None:
        self.assertTrue(self.long.isdisjoint(EmptyDateRange()))
        self.assertFalse(self.long.isdisjoint(InfiniteDateRange()))

        self.assertTrue(self.long.isdisjoint(LeftUnboundedDateRange(end=Date(2020, 1, 1))))
        self.assertTrue(self.long.isdisjoint(LeftUnboundedDateRange(end=Date(2020, 2, 29))))
        self.assertFalse(self.long.isdisjoint(LeftUnboundedDateRange(end=Date(2020, 3, 1))))
        self.assertFalse(self.long.isdisjoint(LeftUnboundedDateRange(end=Date(2020, 4, 30))))
        self.assertFalse(self.long.isdisjoint(LeftUnboundedDateRange(end=Date(2020, 5, 1))))
        self.assertFalse(self.long.isdisjoint(LeftUnboundedDateRange(end=Date(2020, 7, 1))))

        self.assertFalse(self.long.isdisjoint(RightUnboundedDateRange(start=Date(2020, 1, 1))))
        self.assertFalse(self.long.isdisjoint(RightUnboundedDateRange(start=Date(2020, 2, 29))))
        self.assertFalse(self.long.isdisjoint(RightUnboundedDateRange(start=Date(2020, 3, 1))))
        self.assertFalse(self.long.isdisjoint(RightUnboundedDateRange(start=Date(2020, 4, 30))))
        self.assertTrue(self.long.isdisjoint(RightUnboundedDateRange(start=Date(2020, 5, 1))))
        self.assertTrue(self.long.isdisjoint(RightUnboundedDateRange(start=Date(2020, 7, 1))))

        self.assertTrue(
            self.long.isdisjoint(BoundedDateRange(Date(2020, 1, 1), Date(2020, 1, 31))),
        )
        self.assertTrue(
            self.long.isdisjoint(BoundedDateRange(Date(2020, 1, 1), Date(2020, 2, 29))),
        )
        self.assertFalse(
            self.long.isdisjoint(BoundedDateRange(Date(2020, 1, 1), Date(2020, 3, 1))),
        )
        self.assertFalse(
            self.long.isdisjoint(BoundedDateRange(Date(2020, 1, 1), Date(2020, 4, 1))),
        )

        self.assertFalse(self.long.isdisjoint(self.long))
        self.assertFalse(self.long.isdisjoint(self.short))
        self.assertFalse(self.short.isdisjoint(self.long))

        self.assertFalse(
            self.long.isdisjoint(BoundedDateRange(Date(2020, 4, 1), Date(2020, 7, 1))),
        )
        self.assertFalse(
            self.long.isdisjoint(BoundedDateRange(Date(2020, 4, 30), Date(2020, 7, 1))),
        )
        self.assertTrue(
            self.long.isdisjoint(BoundedDateRange(Date(2020, 5, 1), Date(2020, 7, 1))),
        )
        self.assertTrue(
            self.long.isdisjoint(BoundedDateRange(Date(2020, 6, 1), Date(2020, 7, 1))),
        )

    def test_issubset(self) -> None:
        self.assertFalse(self.long.issubset(EmptyDateRange()))
        self.assertTrue(self.long.issubset(InfiniteDateRange()))

        self.assertFalse(self.long.issubset(LeftUnboundedDateRange(end=Date(2020, 1, 1))))
        self.assertFalse(self.long.issubset(LeftUnboundedDateRange(end=Date(2020, 2, 29))))
        self.assertFalse(self.long.issubset(LeftUnboundedDateRange(end=Date(2020, 3, 1))))
        self.assertTrue(self.long.issubset(LeftUnboundedDateRange(end=Date(2020, 4, 30))))
        self.assertTrue(self.long.issubset(LeftUnboundedDateRange(end=Date(2020, 5, 1))))
        self.assertTrue(self.long.issubset(LeftUnboundedDateRange(end=Date(2020, 7, 1))))

        self.assertTrue(self.long.issubset(RightUnboundedDateRange(start=Date(2020, 1, 1))))
        self.assertTrue(self.long.issubset(RightUnboundedDateRange(start=Date(2020, 2, 29))))
        self.assertTrue(self.long.issubset(RightUnboundedDateRange(start=Date(2020, 3, 1))))
        self.assertFalse(self.long.issubset(RightUnboundedDateRange(start=Date(2020, 4, 30))))
        self.assertFalse(self.long.issubset(RightUnboundedDateRange(start=Date(2020, 5, 1))))
        self.assertFalse(self.long.issubset(RightUnboundedDateRange(start=Date(2020, 7, 1))))

        self.assertFalse(
            self.long.issubset(BoundedDateRange(Date(2020, 1, 1), Date(2020, 1, 31))),
        )
        self.assertFalse(
            self.long.issubset(BoundedDateRange(Date(2020, 1, 1), Date(2020, 2, 29))),
        )
        self.assertFalse(
            self.long.issubset(BoundedDateRange(Date(2020, 1, 1), Date(2020, 3, 1))),
        )
        self.assertFalse(
            self.long.issubset(BoundedDateRange(Date(2020, 1, 1), Date(2020, 4, 1))),
        )

        self.assertTrue(self.long.issubset(self.long))
        self.assertFalse(self.long.issubset(self.short))
        self.assertTrue(self.short.issubset(self.long))

        self.assertFalse(
            self.long.issubset(BoundedDateRange(Date(2020, 4, 1), Date(2020, 7, 1))),
        )
        self.assertFalse(
            self.long.issubset(BoundedDateRange(Date(2020, 4, 30), Date(2020, 7, 1))),
        )
        self.assertFalse(
            self.long.issubset(BoundedDateRange(Date(2020, 5, 1), Date(2020, 7, 1))),
        )
        self.assertFalse(
            self.long.issubset(BoundedDateRange(Date(2020, 6, 1), Date(2020, 7, 1))),
        )

    def test_union(self) -> None:
        self.assertEqual(self.long.union(EmptyDateRange()), self.long)
        self.assertEqual(self.long.union(InfiniteDateRange()), InfiniteDateRange())

        with self.assertRaises(ArithmeticError):
            self.long.union(LeftUnboundedDateRange(end=Date(2020, 1, 1)))
        self.assertEqual(
            self.long.union(LeftUnboundedDateRange(end=Date(2020, 2, 29))),
            LeftUnboundedDateRange(end=Date(2020, 4, 30)),
        )
        self.assertEqual(
            self.long.union(LeftUnboundedDateRange(end=Date(2020, 3, 1))),
            LeftUnboundedDateRange(end=Date(2020, 4, 30)),
        )
        self.assertEqual(
            self.long.union(LeftUnboundedDateRange(end=Date(2020, 4, 30))),
            LeftUnboundedDateRange(end=Date(2020, 4, 30)),
        )
        self.assertEqual(
            self.long.union(LeftUnboundedDateRange(end=Date(2020, 5, 1))),
            LeftUnboundedDateRange(end=Date(2020, 5, 1)),
        )
        self.assertEqual(
            self.long.union(LeftUnboundedDateRange(end=Date(2020, 7, 1))),
            LeftUnboundedDateRange(end=Date(2020, 7, 1)),
        )

        self.assertEqual(
            self.long.union(RightUnboundedDateRange(start=Date(2020, 1, 1))),
            RightUnboundedDateRange(start=Date(2020, 1, 1)),
        )
        self.assertEqual(
            self.long.union(RightUnboundedDateRange(start=Date(2020, 2, 29))),
            RightUnboundedDateRange(start=Date(2020, 2, 29)),
        )
        self.assertEqual(
            self.long.union(RightUnboundedDateRange(start=Date(2020, 3, 1))),
            RightUnboundedDateRange(start=Date(2020, 3, 1)),
        )
        self.assertEqual(
            self.long.union(RightUnboundedDateRange(start=Date(2020, 4, 30))),
            RightUnboundedDateRange(start=Date(2020, 3, 1)),
        )
        self.assertEqual(
            self.long.union(RightUnboundedDateRange(start=Date(2020, 5, 1))),
            RightUnboundedDateRange(start=Date(2020, 3, 1)),
        )
        with self.assertRaises(ArithmeticError):
            self.long.union(RightUnboundedDateRange(start=Date(2020, 7, 1)))

        with self.assertRaises(ArithmeticError):
            self.long.union(BoundedDateRange(Date(2020, 1, 1), Date(2020, 1, 31)))
        self.assertEqual(
            self.long.union(BoundedDateRange(Date(2020, 1, 1), Date(2020, 2, 29))),
            BoundedDateRange(Date(2020, 1, 1), Date(2020, 4, 30)),
        )
        self.assertEqual(
            self.long.union(BoundedDateRange(Date(2020, 1, 1), Date(2020, 3, 1))),
            BoundedDateRange(Date(2020, 1, 1), Date(2020, 4, 30)),
        )
        self.assertEqual(
            self.long.union(BoundedDateRange(Date(2020, 1, 1), Date(2020, 4, 1))),
            BoundedDateRange(Date(2020, 1, 1), Date(2020, 4, 30)),
        )

        self.assertEqual(self.long.union(self.long), self.long)
        self.assertEqual(self.long.union(self.short), self.long)
        self.assertEqual(self.short.union(self.long), self.long)

        self.assertEqual(
            self.long.union(BoundedDateRange(Date(2020, 4, 1), Date(2020, 7, 1))),
            BoundedDateRange(Date(2020, 3, 1), Date(2020, 7, 1)),
        )
        self.assertEqual(
            self.long.union(BoundedDateRange(Date(2020, 4, 30), Date(2020, 7, 1))),
            BoundedDateRange(Date(2020, 3, 1), Date(2020, 7, 1)),
        )
        self.assertEqual(
            self.long.union(BoundedDateRange(Date(2020, 5, 1), Date(2020, 7, 1))),
            BoundedDateRange(Date(2020, 3, 1), Date(2020, 7, 1)),
        )
        with self.assertRaises(ArithmeticError):
            self.long.union(BoundedDateRange(Date(2020, 6, 1), Date(2020, 7, 1)))

    def test_difference(self) -> None:
        self.assertEqual(self.long.difference(EmptyDateRange()), self.long)
        self.assertEqual(self.long.difference(InfiniteDateRange()), EmptyDateRange())

        self.assertEqual(
            self.long.difference(LeftUnboundedDateRange(end=Date(2020, 1, 1))),
            self.long,
        )
        self.assertEqual(
            self.long.difference(LeftUnboundedDateRange(end=Date(2020, 2, 29))),
            self.long,
        )
        self.assertEqual(
            self.long.difference(LeftUnboundedDateRange(end=Date(2020, 3, 1))),
            BoundedDateRange(Date(2020, 3, 2), Date(2020, 4, 30)),
        )
        self.assertEqual(
            self.long.difference(LeftUnboundedDateRange(end=Date(2020, 3, 31))),
            BoundedDateRange(Date(2020, 4, 1), Date(2020, 4, 30)),
        )
        self.assertEqual(
            self.long.difference(LeftUnboundedDateRange(end=Date(2020, 4, 30))),
            EmptyDateRange(),
        )
        self.assertEqual(
            self.long.difference(LeftUnboundedDateRange(end=Date(2020, 5, 1))),
            EmptyDateRange(),
        )
        self.assertEqual(
            self.long.difference(LeftUnboundedDateRange(end=Date(2020, 7, 1))),
            EmptyDateRange(),
        )

        self.assertEqual(
            self.long.difference(RightUnboundedDateRange(start=Date(2020, 1, 1))),
            EmptyDateRange(),
        )
        self.assertEqual(
            self.long.difference(RightUnboundedDateRange(start=Date(2020, 2, 29))),
            EmptyDateRange(),
        )
        self.assertEqual(
            self.long.difference(RightUnboundedDateRange(start=Date(2020, 3, 1))),
            EmptyDateRange(),
        )
        self.assertEqual(
            self.long.difference(RightUnboundedDateRange(start=Date(2020, 4, 1))),
            BoundedDateRange(Date(2020, 3, 1), Date(2020, 3, 31)),
        )
        self.assertEqual(
            self.long.difference(RightUnboundedDateRange(start=Date(2020, 4, 30))),
            BoundedDateRange(Date(2020, 3, 1), Date(2020, 4, 29)),
        )
        self.assertEqual(
            self.long.difference(RightUnboundedDateRange(start=Date(2020, 5, 1))),
            self.long,
        )
        self.assertEqual(
            self.long.difference(RightUnboundedDateRange(start=Date(2020, 7, 1))),
            self.long,
        )

        self.assertEqual(
            self.long.difference(BoundedDateRange(Date(2020, 1, 1), Date(2020, 1, 31))),
            self.long,
        )
        self.assertEqual(
            self.long.difference(BoundedDateRange(Date(2020, 1, 1), Date(2020, 2, 29))), self.long
        )
        self.assertEqual(
            self.long.difference(BoundedDateRange(Date(2020, 1, 1), Date(2020, 3, 1))),
            BoundedDateRange(Date(2020, 3, 2), Date(2020, 4, 30)),
        )
        self.assertEqual(
            self.long.difference(BoundedDateRange(Date(2020, 1, 1), Date(2020, 4, 1))),
            BoundedDateRange(Date(2020, 4, 2), Date(2020, 4, 30)),
        )
        self.assertEqual(
            self.long.difference(BoundedDateRange(Date(2020, 1, 1), Date(2020, 4, 30))),
            EmptyDateRange(),
        )
        self.assertEqual(
            self.long.difference(BoundedDateRange(Date(2020, 1, 1), Date(2020, 5, 1))),
            EmptyDateRange(),
        )

        self.assertEqual(self.long.difference(self.long), EmptyDateRange())
        self.assertEqual(self.short.difference(self.long), EmptyDateRange())
        with self.assertRaises(ArithmeticError):
            self.long.difference(BoundedDateRange(Date(2020, 3, 20), Date(2020, 4, 7)))

        self.assertEqual(
            self.long.difference(BoundedDateRange(Date(2020, 1, 1), Date(2020, 7, 1))),
            EmptyDateRange(),
        )
        self.assertEqual(
            self.long.difference(BoundedDateRange(Date(2020, 3, 1), Date(2020, 7, 1))),
            EmptyDateRange(),
        )
        self.assertEqual(
            self.long.difference(BoundedDateRange(Date(2020, 4, 1), Date(2020, 7, 1))),
            BoundedDateRange(Date(2020, 3, 1), Date(2020, 3, 31)),
        )
        self.assertEqual(
            self.long.difference(BoundedDateRange(Date(2020, 4, 30), Date(2020, 7, 1))),
            BoundedDateRange(Date(2020, 3, 1), Date(2020, 4, 29)),
        )
        self.assertEqual(
            self.long.difference(BoundedDateRange(Date(2020, 5, 1), Date(2020, 7, 1))),
            self.long,
        )
        self.assertEqual(
            self.long.difference(BoundedDateRange(Date(2020, 6, 1), Date(2020, 7, 1))),
            self.long,
        )
