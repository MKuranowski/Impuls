from typing import Type

from impuls.model import ShapePoint

from .template_entity import AbstractTestEntity


class TestShapePoint(AbstractTestEntity.Template[ShapePoint]):
    def get_entity(self) -> ShapePoint:
        return ShapePoint(
            shape_id="Sh0",
            sequence=0,
            lat=1.5,
            lon=-3.14,
        )

    def get_type(self) -> Type[ShapePoint]:
        return ShapePoint

    def test_sql_marshall(self) -> None:
        self.assertTupleEqual(
            self.get_entity().sql_marshall(),
            ("Sh0", 0, 1.5, -3.14, None),
        )

    def test_sql_marshall_shape_dist_traveled(self) -> None:
        st = self.get_entity()
        st.shape_dist_traveled = 5.1

        self.assertTupleEqual(
            st.sql_marshall(),
            ("Sh0", 0, 1.5, -3.14, 5.1),
        )

    def test_sql_primary_key(self) -> None:
        self.assertTupleEqual(self.get_entity().sql_primary_key(), ("Sh0", 0))

    def test_sql_unmarshall(self) -> None:
        sp = ShapePoint.sql_unmarshall(("Sh0", 0, 1.5, -3.14, None))

        self.assertEqual(sp.shape_id, "Sh0")
        self.assertEqual(sp.sequence, 0)
        self.assertEqual(sp.lat, 1.5)
        self.assertEqual(sp.lon, -3.14)
        self.assertIsNone(sp.shape_dist_traveled)

    def test_sql_unmarshall_shape_dist_traveled(self) -> None:
        sp = ShapePoint.sql_unmarshall(("Sh0", 0, 1.5, -3.14, 5.1))
        self.assertEqual(sp.shape_dist_traveled, 5.1)
