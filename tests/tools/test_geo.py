from unittest import TestCase

from impuls.tools.geo import earth_distance_m


class TestEarthDistanceM(TestCase):
    def test(self) -> None:
        self.assertAlmostEqual(
            earth_distance_m(52.23024, 21.01062, 52.23852, 21.0446),
            2490.5,
            delta=0.1,
        )
        self.assertAlmostEqual(
            earth_distance_m(52.23024, 21.01062, 52.16125, 21.21147),
            15692.5,
            delta=0.1,
        )
