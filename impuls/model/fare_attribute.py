from dataclasses import dataclass, field
from enum import IntEnum
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
class FareAttribute(Entity):
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

    @staticmethod
    def gtfs_table_name() -> LiteralString:
        return "fare_attributes"

    def gtfs_marshall(self) -> dict[str, str]:
        return {
            "fare_id": self.id,
            "price": str(self.price),
            "currency_type": self.currency_type,
            "payment_method": str(self.payment_method.value),
            "transfers": str(self.transfers) if self.transfers is not None else "",
            "agency_id": self.agency_id,
            "transfer_duration": (
                str(self.transfer_duration) if self.transfer_duration is not None else ""
            ),
        }

    @classmethod
    def gtfs_unmarshall(cls: TypeOf[Self], row: Mapping[str, str]) -> Self:
        return cls(
            **DataclassGTFSBuilder(row)
            .field("id", "fare_id")
            .field("price", converter=float)
            .field("currency_type")
            .field("payment_method", converter=lambda x: cls.PaymentMethod(int(x)))
            .field("transfers", converter=lambda x: int(x) if x else None)
            .field("agency_id")
            .field(
                "transfer_duration",
                converter=lambda x: int(x) if x else None,
                fallback_value=None,
            )
            .kwargs()
        )

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
            agency_id TEXT NOT NULL REFERENCES agencies(agency_id),
            transfer_duration INTEGER DEFAULT NULL CHECK (transfer_duration > 0)
        ) STRICT;"""

    @staticmethod
    def sql_placeholder() -> LiteralString:
        return "(?, ?, ?, ?, ?, ?, ?)"

    @staticmethod
    def sql_where_clause() -> LiteralString:
        return "fare_id = ?"

    @staticmethod
    def sql_set_clause() -> LiteralString:
        return (
            "fare_id = ?, price = ?, currency_type = ?, payment_method = ?, transfers = ?, "
            "agency_id = ?, transfer_duration = ?"
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
            .field("payment_method", int, lambda x: cls.PaymentMethod(x))
            .field("transfers", int, nullable=True)
            .field("agency_id", str)
            .field("transfer_duration", int, nullable=True)
            .kwargs()
        )
