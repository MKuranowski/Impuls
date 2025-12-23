# © Copyright 2022-2025 Mikołaj Kuranowski
# SPDX-License-Identifier: GPL-3.0-or-later
# cSpell: words dlat dlon

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

    sin_dlat_half = math.sin((lat2 - lat1) * 0.5)
    sin_dlon_half = math.sin((lon2 - lon1) * 0.5)
    h = (
        sin_dlat_half * sin_dlat_half
        + math.cos(lat1) * math.cos(lat2) * sin_dlon_half * sin_dlon_half
    )
    return EARTH_DIAMETER_M * math.asin(math.sqrt(h))


def initial_bearing(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculates the initial bearing when moving from (lat1, lon1) to (lat2, lon2)
    along a geodesic (shortest path). Returns the result in degrees.

    >>> cape_town = -33.9, 18.4
    >>> melbourne = -37.8, 144.9
    >>> initial_bearing(*cape_town, *melbourne)
    140.5123...
    >>> initial_bearing(*melbourne, *cape_town)
    -138.0879...
    """

    # Loosely based on all of the answers in https://stackoverflow.com/questions/54873868/

    lat1 = math.radians(lat1)
    lat2 = math.radians(lat2)
    dlon = math.radians(lon2 - lon1)

    y = math.cos(lat2) * math.sin(dlon)
    x = math.cos(lat1) * math.sin(lat2) - math.sin(lat1) * math.cos(lat2) * math.cos(dlon)
    return math.degrees(math.atan2(y, x))
