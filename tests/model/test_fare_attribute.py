from typing import Type

from impuls.model import FareAttribute

from .template_entity import AbstractTestEntity


class TestFareAttribute(AbstractTestEntity.Template[FareAttribute]):
    def get_entity(self) -> FareAttribute:
        return FareAttribute(
            id="F0",
            price=1.50,
            currency_type="EUR",
            payment_method=FareAttribute.PaymentMethod.ON_BOARD,
            transfers=0,
            agency_id="A0",
            transfer_duration=None,
            extra_fields_json=None,
        )

    def get_type(self) -> Type[FareAttribute]:
        return FareAttribute

    def test_sql_marshall(self) -> None:
        self.assertTupleEqual(
            self.get_entity().sql_marshall(),
            ("F0", 1.5, "EUR", 0, 0, "A0", None, None),
        )

    def test_sql_marshall_unlimited_transfers_max_duration(self) -> None:
        f = self.get_entity()
        f.transfers = None
        f.transfer_duration = 3600

        self.assertTupleEqual(f.sql_marshall(), ("F0", 1.5, "EUR", 0, None, "A0", 3600, None))

    def test_sql_primary_key(self) -> None:
        self.assertTupleEqual(self.get_entity().sql_primary_key(), ("F0",))

    def test_sql_unmarshall(self) -> None:
        f = FareAttribute.sql_unmarshall(("F0", 1.5, "EUR", 0, 0, "A0", None, None))

        self.assertEqual(f.id, "F0")
        self.assertEqual(f.price, 1.5)
        self.assertEqual(f.currency_type, "EUR")
        self.assertEqual(f.payment_method, FareAttribute.PaymentMethod.ON_BOARD)
        self.assertEqual(f.transfers, 0)
        self.assertEqual(f.agency_id, "A0")
        self.assertIsNone(f.transfer_duration)
        self.assertIsNone(f.extra_fields_json)

    def test_sql_unmarshall_unlimited_transfers_max_duration(self) -> None:
        f = FareAttribute.sql_unmarshall(("F0", 1.5, "EUR", 0, None, "A0", 3600, None))

        self.assertEqual(f.id, "F0")
        self.assertEqual(f.price, 1.5)
        self.assertEqual(f.currency_type, "EUR")
        self.assertEqual(f.payment_method, FareAttribute.PaymentMethod.ON_BOARD)
        self.assertIsNone(f.transfers)
        self.assertEqual(f.agency_id, "A0")
        self.assertEqual(f.transfer_duration, 3600)
        self.assertIsNone(f.extra_fields_json)
