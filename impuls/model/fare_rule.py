from dataclasses import dataclass, field
from typing import Mapping, Optional, Sequence
from typing import Type as TypeOf
from typing import final

from typing_extensions import LiteralString

from ..tools.types import Self, SQLNativeType
from .meta.entity import Entity
from .meta.gtfs_builder import DataclassGTFSBuilder
from .meta.sql_builder import DataclassSQLBuilder


@final
@dataclass
class FareRule(Entity):
    fare_id: str
    route_id: str = ""
    origin_id: str = ""
    destination_id: str = ""
    contains_id: str = ""

    id: int = field(default=0, repr=False)
    """This field is ignored on `DBConnection.create` - SQLite automatically generates an ID.

    The GTFS primary key clause is incompatible with SQL, as it contains optional columns
    (in SQL PRIMARY KEY implies NOT NULL) - hence the need for a separate ID.
    """

    @staticmethod
    def gtfs_table_name() -> LiteralString:
        return "fare_rules"

    def gtfs_marshall(self) -> dict[str, str]:
        return {
            "fare_id": self.fare_id,
            "route_id": self.route_id,
            "origin_id": self.origin_id,
            "destination_id": self.destination_id,
            "contains_id": self.contains_id,
        }

    @classmethod
    def gtfs_unmarshall(cls: TypeOf[Self], row: Mapping[str, str]) -> Self:
        return cls(
            **DataclassGTFSBuilder(row)
            .field("fare_id")
            .field("route_id", fallback_value="")
            .field("origin_id", fallback_value="")
            .field("destination_id", fallback_value="")
            .field("contains_id", fallback_value="")
            .kwargs()
        )

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
            .field("route_id", Optional[str], lambda x: x or "")  # type: ignore
            .field("origin_id", Optional[str], lambda x: x or "")  # type: ignore
            .field("destination_id", Optional[str], lambda x: x or "")  # type: ignore
            .field("contains_id", Optional[str], lambda x: x or "")  # type: ignore
            .kwargs()
        )
