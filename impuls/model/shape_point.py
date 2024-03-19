from dataclasses import dataclass, field
from typing import Optional, Sequence
from typing import Type as TypeOf
from typing import final

from typing_extensions import LiteralString

from ..tools.types import Self, SQLNativeType
from .meta.entity import Entity
from .meta.sql_builder import DataclassSQLBuilder


@final
@dataclass
class ShapePoint(Entity):
    shape_id: str
    sequence: int
    lat: float = field(repr=False)
    lon: float = field(repr=False)
    shape_dist_traveled: Optional[float] = field(default=None, repr=False)

    @staticmethod
    def sql_table_name() -> LiteralString:
        return "shape_points"

    @staticmethod
    def sql_create_table() -> LiteralString:
        return """CREATE TABLE shapes (shape_id TEXT PRIMARY KEY) STRICT;
        CREATE TABLE shape_points (
            shape_id TEXT NOT NULL REFERENCES shapes(shape_id) ON DELETE CASCADE ON UPDATE CASCADE,
            sequence INTEGER NOT NULL CHECK (sequence >= 0),
            lat REAL NOT NULL,
            lon REAL NOT NULL,
            shape_dist_traveled REAL DEFAULT NULL,
            PRIMARY KEY (shape_id, sequence)
        ) STRICT;"""

    @staticmethod
    def sql_columns() -> LiteralString:
        return "(shape_id, sequence, lat, lon, shape_dist_traveled)"

    @staticmethod
    def sql_placeholder() -> LiteralString:
        return "(?, ?, ?, ?, ?)"

    @staticmethod
    def sql_where_clause() -> LiteralString:
        return "shape_id = ? AND sequence = ?"

    @staticmethod
    def sql_set_clause() -> LiteralString:
        return "shape_id = ?, sequence = ?, lat = ?, lon = ?, shape_dist_traveled = ?"

    def sql_marshall(self) -> tuple[SQLNativeType, ...]:
        return (self.shape_id, self.sequence, self.lat, self.lon, self.shape_dist_traveled)

    def sql_primary_key(self) -> tuple[SQLNativeType, ...]:
        return (self.shape_id, self.sequence)

    @classmethod
    def sql_unmarshall(cls: TypeOf[Self], row: Sequence[SQLNativeType]) -> Self:
        return cls(
            **DataclassSQLBuilder(row)
            .field("shape_id", str)
            .field("sequence", int)
            .field("lat", float)
            .field("lon", float)
            .field("shape_dist_traveled", float, nullable=True)
            .kwargs()
        )
