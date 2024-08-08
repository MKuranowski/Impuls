# © Copyright 2022-2024 Mikołaj Kuranowski
# SPDX-License-Identifier: GPL-3.0-or-later

from dataclasses import dataclass, field
from typing import Sequence
from typing import Type as TypeOf
from typing import final

from typing_extensions import LiteralString

from ..tools.types import Self, SQLNativeType
from .meta.entity import Entity
from .meta.sql_builder import DataclassSQLBuilder


@final
@dataclass
class FareRule(Entity):
    """FareRule instances restrict how :py:class:`FareAttribute` instances can be applied.

    Equivalent to `GTFS's fare_rules.txt entries <https://gtfs.org/schedule/reference/#fare_rulestxt>`_.

    The GTFS specification is heavily vague on how multiple rules are combined.
    Impuls's author current understanding is that: rules with different :py:attr:`route_id`
    are logically ORed, while all rules with the same :py:attr:`route_id` are logically ANDed,
    both horizontally (across multiple rules) and vertically (across multiple fields).
    For example, the following rules:

    1. ``FareRule("f", route_id="", contains_id="A")``
    2. ``FareRule("f", route_id="100", contains_id="A")``
    3. ``FareRule("f", route_id="100", contains_id="B")``
    4. ``FareRule("f", route_id="200", origin_id="A", destination_id="B")``

    Would be mean that fare ``f`` applies to (all routes if journey is completely within zone
    ``A``) OR (route ``100`` if journey passes exactly through zones ``A`` and ``B``) OR
    (route ``200`` if journey starts in zone ``A`` and ends in zone ``B``
    (regardless if it passes through other zones)).

    Thus, fare ``f`` would apply to journeys on route ``100`` contained within zone ``A``
    (thanks to rule 1) or contained within zones ``A`` and ``B`` (thanks to rules 2 and 3),
    but not within zone ``B`` (as rules 2 and 3 are logically ANDed). Similarly, fare ``f``
    would apply on journeys on route ``200`` contained within zone ``A`` (thanks to rule 1);
    starting in zone ``A``, passing through zone ``C`` and ending in zone ``B`` (thanks to rule 4);
    but not starting in zone ``B`` and ending in zone ``A`` (as rule 4 is directional).

    Essentially, ``origin_id`` and ``destination_id`` applied to the :py:attr:`Stop.zone_id`
    of the embarking and disembarking stops of a user's journey leg; while ``contains_id``
    applies to all :py:attr:`Stop.zone_id` between the embarking and disembarking stops, inclusive.
    """  # noqa: E501

    fare_id: str
    route_id: str = ""
    origin_id: str = ""
    destination_id: str = ""
    contains_id: str = ""

    id: int = field(default=0, repr=False)
    """This field is ignored on :py:meth:`DBConnection.create` -
    SQLite automatically generates an ID.

    The GTFS primary key clause is incompatible with SQL, as it contains optional columns
    (in SQL PRIMARY KEY implies NOT NULL) - hence the need for a separate ID.
    """

    @staticmethod
    def sql_table_name() -> LiteralString:
        return "fare_rules"

    @staticmethod
    def sql_create_table() -> LiteralString:
        return """CREATE TABLE fare_rules (
            fare_rule_id INTEGER PRIMARY KEY,
            fare_id TEXT NOT NULL REFERENCES fare_attributes(fare_id)
                ON DELETE CASCADE ON UPDATE CASCADE,
            route_id TEXT DEFAULT NULL REFERENCES routes(route_id)
                ON DELETE CASCADE ON UPDATE CASCADE,
            origin_id TEXT DEFAULT NULL,
            destination_id TEXT DEFAULT NULL,
            contains_id TEXT DEFAULT NULL
        ) STRICT;
        CREATE INDEX idx_fare_rules_route_id ON fare_rules(route_id);
        CREATE INDEX idx_fare_rules_origin_id ON fare_rules(origin_id);
        CREATE INDEX idx_fare_rules_destination_id ON fare_rules(destination_id);
        CREATE INDEX idx_fare_rules_contains_id ON fare_rules(contains_id);"""

    @staticmethod
    def sql_columns() -> LiteralString:
        return "(fare_id, route_id, origin_id, destination_id, contains_id)"

    @staticmethod
    def sql_placeholder() -> LiteralString:
        return "(?, ?, ?, ?, ?)"

    @staticmethod
    def sql_where_clause() -> LiteralString:
        return "fare_rule_id = ?"

    @staticmethod
    def sql_set_clause() -> LiteralString:
        return "fare_id = ?, route_id = ?, origin_id = ?, destination_id = ?, contains_id = ?"

    def sql_marshall(self) -> tuple[SQLNativeType, ...]:
        return (
            self.fare_id,
            self.route_id or None,
            self.origin_id or None,
            self.destination_id or None,
            self.contains_id or None,
        )

    def sql_primary_key(self) -> tuple[SQLNativeType, ...]:
        return (self.id,)

    @classmethod
    def sql_unmarshall(cls: TypeOf[Self], row: Sequence[SQLNativeType]) -> Self:
        return cls(
            **DataclassSQLBuilder(row)
            .field("id", int)
            .field("fare_id", str)
            .optional_field("route_id", str, lambda x: x or "")
            .optional_field("origin_id", str, lambda x: x or "")
            .optional_field("destination_id", str, lambda x: x or "")
            .optional_field("contains_id", str, lambda x: x or "")
            .kwargs()
        )
