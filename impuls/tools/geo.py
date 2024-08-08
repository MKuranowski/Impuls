# © Copyright 2022-2024 Mikołaj Kuranowski
# SPDX-License-Identifier: GPL-3.0-or-later

import math

EARTH_RADIUS_M = 6_371_008.8
"""Mean Earth radius, 6 371 008.8 meters.
Source: https://en.wikipedia.org/wiki/Earth_radius#Arithmetic_mean_radius
"""

EARTH_DIAMETER_M = EARTH_RADIUS_M + EARTH_RADIUS_M
"""Mean Earth diameter, double of :py:const:`EARTH_RADIUS_M`."""


def earth_distance_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculates the distance on earth using the
    `Haversine formula <https://en.wikipedia.org/wiki/Haversine_formula>`_.
    Returns the result in meters.
    """
    lat1 = math.radians(lat1)
    lon1 = math.radians(lon1)
    lat2 = math.radians(lat2)
    lon2 = math.radians(lon2)

    # cSpell: words dlat dlon
    sin_dlat_half = math.sin((lat2 - lat1) * 0.5)
    sin_dlon_half = math.sin((lon2 - lon1) * 0.5)
    h = (
        sin_dlat_half * sin_dlat_half
        + math.cos(lat1) * math.cos(lat2) * sin_dlon_half * sin_dlon_half
    )
    return EARTH_DIAMETER_M * math.asin(math.sqrt(h))
