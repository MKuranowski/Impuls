from typing import Type, final

from impuls.model import FareAttribute

from .template_entity import AbstractTestEntity


@final
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
        )

    def get_type(self) -> Type[FareAttribute]:
        return FareAttribute

    def test_gtfs_marshall(self) -> None:
        self.assertDictEqual(
            self.get_entity().gtfs_marshall(),
            {
                "fare_id": "F0",
                "price": "1.5",
                "currency_type": "EUR",
                "payment_method": "0",
                "transfers": "0",
                "agency_id": "A0",
                "transfer_duration": "",
            },
        )

    def test_gtfs_marshall_unlimited_transfers_max_duration(self) -> None:
        f = self.get_entity()
        f.transfers = None
        f.transfer_duration = 3600

        m = f.gtfs_marshall()
        self.assertEqual(m["transfers"], "")
        self.assertEqual(m["transfer_duration"], "3600")

    def test_gtfs_unmarshall(self) -> None:
        f = FareAttribute.gtfs_unmarshall(
            {
                "fare_id": "F0",
                "price": "1.5",
                "currency_type": "EUR",
                "payment_method": "0",
                "transfers": "0",
                "agency_id": "A0",
                "transfer_duration": "",
            },
        )

        self.assertEqual(f.id, "F0")
        self.assertEqual(f.price, 1.5)
        self.assertEqual(f.currency_type, "EUR")
        self.assertEqual(f.payment_method, FareAttribute.PaymentMethod.ON_BOARD)
        self.assertEqual(f.transfers, 0)
        self.assertEqual(f.agency_id, "A0")
        self.assertIsNone(f.transfer_duration)

    def test_gtfs_unmarshall_unlimited_transfers_max_duration(self) -> None:
        f = FareAttribute.gtfs_unmarshall(
            {
                "fare_id": "F0",
                "price": "1.5",
                "currency_type": "EUR",
                "payment_method": "0",
                "transfers": "",
                "agency_id": "A0",
                "transfer_duration": "3600",
            },
        )

        self.assertEqual(f.id, "F0")
        self.assertEqual(f.price, 1.5)
        self.assertEqual(f.currency_type, "EUR")
        self.assertEqual(f.payment_method, FareAttribute.PaymentMethod.ON_BOARD)
        self.assertIsNone(f.transfers)
        self.assertEqual(f.agency_id, "A0")
        self.assertEqual(f.transfer_duration, 3600)

    def test_sql_marshall(self) -> None:
        self.assertTupleEqual(
            self.get_entity().sql_marshall(),
            ("F0", 1.5, "EUR", 0, 0, "A0", None),
        )

    def test_sql_marshall_unlimited_transfers_max_duration(self) -> None:
        f = self.get_entity()
        f.transfers = None
        f.transfer_duration = 3600

        self.assertTupleEqual(f.sql_marshall(), ("F0", 1.5, "EUR", 0, None, "A0", 3600))

    def test_sql_primary_key(self) -> None:
        self.assertTupleEqual(self.get_entity().sql_primary_key(), ("F0",))

    def test_sql_unmarshall(self) -> None:
        f = FareAttribute.sql_unmarshall(("F0", 1.5, "EUR", 0, 0, "A0", None))

        self.assertEqual(f.id, "F0")
        self.assertEqual(f.price, 1.5)
        self.assertEqual(f.currency_type, "EUR")
        self.assertEqual(f.payment_method, FareAttribute.PaymentMethod.ON_BOARD)
        self.assertEqual(f.transfers, 0)
        self.assertEqual(f.agency_id, "A0")
        self.assertIsNone(f.transfer_duration)

    def test_sql_unmarshall_unlimited_transfers_max_duration(self) -> None:
        f = FareAttribute.sql_unmarshall(("F0", 1.5, "EUR", 0, None, "A0", 3600))

        self.assertEqual(f.id, "F0")
        self.assertEqual(f.price, 1.5)
        self.assertEqual(f.currency_type, "EUR")
        self.assertEqual(f.payment_method, FareAttribute.PaymentMethod.ON_BOARD)
        self.assertIsNone(f.transfers)
        self.assertEqual(f.agency_id, "A0")
        self.assertEqual(f.transfer_duration, 3600)