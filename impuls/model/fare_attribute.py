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
class FareAttribute(Entity, ExtraFieldsMixin):
    """FareAttributes define a single logical fare class. Due to the way :py:class:`FareRule`
    is applied, there may be multiple FareAttributes representing the same "ticket".

    Equivalent to `GTFS's fare_attributes.txt entries <https://gtfs.org/schedule/reference/#fare_attributestxt>`_.
    """  # noqa: E501

    class PaymentMethod(IntEnum):
        ON_BOARD = 0
        BEFORE_BOARDING = 1

    id: str
    price: float
    currency_type: str = field(repr=False)
    payment_method: PaymentMethod = field(repr=False)
    transfers: Optional[int]
    agency_id: str = field(repr=False)
    transfer_duration: Optional[int] = field(default=None)
    extra_fields_json: Optional[str] = field(default=None, repr=False)

    @staticmethod
    def sql_table_name() -> LiteralString:
        return "fare_attributes"

    @staticmethod
    def sql_create_table() -> LiteralString:
        return """CREATE TABLE fare_attributes (
            fare_id TEXT PRIMARY KEY,
            price REAL NOT NULL CHECK (price >= 0.0),
            currency_type TEXT NOT NULL CHECK (currency_type LIKE '___'),
            payment_method INTEGER NOT NULL CHECK (payment_method IN (0, 1)),
            transfers INTEGER DEFAULT NULL CHECK (transfers IN (0, 1, 2)),
            agency_id TEXT NOT NULL REFERENCES agencies(agency_id)
                ON DELETE CASCADE ON UPDATE CASCADE,
            transfer_duration INTEGER DEFAULT NULL CHECK (transfer_duration > 0),
            extra_fields_json TEXT DEFAULT NULL
        ) STRICT;"""

    @staticmethod
    def sql_columns() -> LiteralString:
        return (
            "(fare_id, price, currency_type, payment_method, transfers, agency_id, "
            "transfer_duration, extra_fields_json)"
        )

    @staticmethod
    def sql_placeholder() -> LiteralString:
        return "(?, ?, ?, ?, ?, ?, ?, ?)"

    @staticmethod
    def sql_where_clause() -> LiteralString:
        return "fare_id = ?"

    @staticmethod
    def sql_set_clause() -> LiteralString:
        return (
            "fare_id = ?, price = ?, currency_type = ?, payment_method = ?, transfers = ?, "
            "agency_id = ?, transfer_duration = ?, extra_fields_json = ?"
        )

    def sql_marshall(self) -> tuple[SQLNativeType, ...]:
        return (
            self.id,
            self.price,
            self.currency_type,
            self.payment_method.value,
            self.transfers,
            self.agency_id,
            self.transfer_duration,
            self.extra_fields_json,
        )

    def sql_primary_key(self) -> tuple[SQLNativeType, ...]:
        return (self.id,)

    @classmethod
    def sql_unmarshall(cls: TypeOf[Self], row: Sequence[SQLNativeType]) -> Self:
        return cls(
            **DataclassSQLBuilder(row)
            .field("id", str)
            .field("price", float)
            .field("currency_type", str)
            .field("payment_method", int, cls.PaymentMethod)
            .nullable_field("transfers", int)
            .field("agency_id", str)
            .nullable_field("transfer_duration", int)
            .nullable_field("extra_fields_json", str)
            .kwargs()
        )
