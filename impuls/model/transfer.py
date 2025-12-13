# © Copyright 2022-2024 Mikołaj Kuranowski
# SPDX-License-Identifier: GPL-3.0-or-later

from dataclasses import dataclass, field
from enum import IntEnum
from typing import Optional, Sequence
from typing import Type as TypeOf
from typing import final

from typing_extensions import LiteralString

from ..tools.types import Self, SQLNativeType
from .meta.entity import Entity
from .meta.extra_fields_mixin import ExtraFieldsMixin
from .meta.sql_builder import DataclassSQLBuilder


@final
@dataclass
class Transfer(Entity, ExtraFieldsMixin):
    """Transfer represent special rules for transferring between vehicles on the network.

    Equivalent to `GTFS's transfers.txt entries <https://gtfs.org/schedule/reference/#transferstxt>`_.
    """  # noqa: E501

    class Type(IntEnum):
        RECOMMENDED = 0
        TIMED = 1
        MIN_TIME_REQUIRED = 2
        IMPOSSIBLE = 3
        IN_SEAT = 4
        RE_BOARD = 5

    from_stop_id: str = ""
    to_stop_id: str = ""
    from_route_id: str = ""
    to_route_id: str = ""
    from_trip_id: str = ""
    to_trip_id: str = ""
    type: Type = Type.RECOMMENDED
    min_transfer_time: Optional[int] = field(default=None, repr=False)
    extra_fields_json: Optional[str] = field(default=None, repr=False)

    id: int = field(default=0, repr=False)
    """This field is ignored on :py:meth:`DBConnection.create` -
    SQLite automatically generates an ID.

    The GTFS primary key clause is incompatible with SQL, as it contains optional columns
    (in SQL PRIMARY KEY implies NOT NULL) - hence the need for a separate ID.
    """

    @staticmethod
    def sql_table_name() -> LiteralString:
        return "transfers"

    @staticmethod
    def sql_create_table() -> LiteralString:
        return """CREATE TABLE transfers (
            transfer_id INTEGER PRIMARY KEY,
            from_stop_id TEXT DEFAULT NULL REFERENCES stops(stop_id)
                ON DELETE CASCADE ON UPDATE CASCADE
                CHECK (from_stop_id IS NOT NULL OR transfer_type IN (4, 5)),
            to_stop_id TEXT DEFAULT NULL REFERENCES stops(stop_id)
                ON DELETE CASCADE ON UPDATE CASCADE
                CHECK (to_stop_id IS NOT NULL OR transfer_type IN (4, 5)),
            from_route_id TEXT DEFAULT NULL REFERENCES routes(route_id)
                ON DELETE CASCADE ON UPDATE CASCADE,
            to_route_id TEXT DEFAULT NULL REFERENCES routes(route_id)
                ON DELETE CASCADE ON UPDATE CASCADE,
            from_trip_id TEXT DEFAULT NULL REFERENCES trips(trip_id)
                ON DELETE CASCADE ON UPDATE CASCADE
                CHECK (from_trip_id IS NOT NULL OR transfer_type IN (0, 1, 2, 3)),
            to_trip_id TEXT DEFAULT NULL REFERENCES trips(trip_id)
                ON DELETE CASCADE ON UPDATE CASCADE
                CHECK (to_trip_id IS NOT NULL OR transfer_type IN (0, 1, 2, 3)),
            transfer_type INTEGER NOT NULL DEFAULT 0 CHECK (transfer_type IN (0, 1, 2, 3, 4, 5)),
            min_transfer_time INTEGER DEFAULT NULL CHECK (min_transfer_time > 0),
            extra_fields_json TEXT DEFAULT NULL
        ) STRICT;
        CREATE INDEX idx_transfers_to_stop_id ON transfers(to_stop_id);
        CREATE INDEX idx_transfers_from_route_id ON transfers(from_route_id);
        CREATE INDEX idx_transfers_to_route_id ON transfers(to_route_id);
        CREATE INDEX idx_transfers_from_trip_id ON transfers(from_trip_id);
        CREATE INDEX idx_transfers_to_trip_id ON transfers(to_trip_id);"""

    @staticmethod
    def sql_columns() -> LiteralString:
        return (
            "(from_stop_id, to_stop_id, from_route_id, to_route_id, from_trip_id, to_trip_id, "
            "transfer_type, min_transfer_time, extra_fields_json)"
        )

    @staticmethod
    def sql_placeholder() -> LiteralString:
        return "(?, ?, ?, ?, ?, ?, ?, ?, ?)"

    @staticmethod
    def sql_where_clause() -> LiteralString:
        return "transfer_id = ?"

    @staticmethod
    def sql_set_clause() -> LiteralString:
        return (
            "from_stop_id = ?, to_stop_id = ?, from_route_id = ?, to_route_id = ?, "
            "from_trip_id = ?, to_trip_id = ?, transfer_type = ?, min_transfer_time = ?, "
            "extra_fields_json = ?"
        )

    def sql_marshall(self) -> tuple[SQLNativeType, ...]:
        return (
            self.from_stop_id or None,
            self.to_stop_id or None,
            self.from_route_id or None,
            self.to_route_id or None,
            self.from_trip_id or None,
            self.to_trip_id or None,
            self.type.value,
            self.min_transfer_time,
            self.extra_fields_json,
        )

    def sql_primary_key(self) -> tuple[SQLNativeType, ...]:
        return (self.id,)

    @classmethod
    def sql_unmarshall(cls: TypeOf[Self], row: Sequence[SQLNativeType]) -> Self:
        return cls(
            **DataclassSQLBuilder(row)
            .field("id", int)
            .optional_field("from_stop_id", str, lambda x: x or "")
            .optional_field("to_stop_id", str, lambda x: x or "")
            .optional_field("from_route_id", str, lambda x: x or "")
            .optional_field("to_route_id", str, lambda x: x or "")
            .optional_field("from_trip_id", str, lambda x: x or "")
            .optional_field("to_trip_id", str, lambda x: x or "")
            .field("type", int, cls.Type)
            .nullable_field("min_transfer_time", int)
            .nullable_field("extra_fields_json", str)
            .kwargs()
        )
