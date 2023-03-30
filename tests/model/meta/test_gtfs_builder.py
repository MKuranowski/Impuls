import unittest

from impuls.model.meta.gtfs_builder import (
    DataclassGTFSBuilder,
    InvalidGTFSCellValue,
    MissingGTFSColumn,
)


class TestDataclassGTFSBuilder(unittest.TestCase):
    def test(self) -> None:
        b = DataclassGTFSBuilder({"id": "1", "name": "foo"})
        b.field("id")
        b.field("name")
        d = b.kwargs()

        self.assertDictEqual(d, {"id": "1", "name": "foo"})

    def test_missing(self) -> None:
        with self.assertRaisesRegex(MissingGTFSColumn, r"name"):
            DataclassGTFSBuilder({"id": "1"}).field("id").field("name").kwargs()

    def test_gtfs_col(self) -> None:
        b = DataclassGTFSBuilder({"stop_id": "1", "stop_name": "foo"})
        b.field("id", gtfs_col="stop_id")
        b.field("name", gtfs_col="stop_name")
        d = b.kwargs()

        self.assertDictEqual(d, {"id": "1", "name": "foo"})

    def test_missing_gtfs_col(self) -> None:
        with self.assertRaisesRegex(MissingGTFSColumn, r"stop_name"):
            (
                DataclassGTFSBuilder({"stop_id": "1"})
                .field("id", "stop_id")
                .field("name", "stop_name")
                .kwargs()
            )

    def test_fallback_value(self) -> None:
        self.assertDictEqual(
            (
                DataclassGTFSBuilder({"id": "1"})
                .field("id")
                .field("name", fallback_value=None)
                .kwargs()
            ),
            {"id": "1", "name": None},
        )

        self.assertDictEqual(
            (
                DataclassGTFSBuilder({"id": "1", "name": "foo"})
                .field("id")
                .field("name", fallback_value=None)
                .kwargs()
            ),
            {"id": "1", "name": "foo"},
        )

    def test_converter(self) -> None:
        d = DataclassGTFSBuilder({"id": "1"}).field("id", converter=int).kwargs()
        self.assertEqual(d["id"], 1)

    def test_converter_raises(self) -> None:
        with self.assertRaises(InvalidGTFSCellValue) as cm:
            DataclassGTFSBuilder({"id": "foo"}).field("id", converter=int).kwargs()
        self.assertEqual(cm.exception.col, "id")
        self.assertEqual(cm.exception.value, "foo")

    def test_converter_fallback_value(self) -> None:
        d = DataclassGTFSBuilder({}).field("id", converter=int, fallback_value=None).kwargs()
        self.assertEqual(d["id"], None)

    def test_converter_fallback_value_still_raises(self) -> None:
        with self.assertRaises(InvalidGTFSCellValue) as cm:
            (
                DataclassGTFSBuilder({"id": "foo"})
                .field("id", converter=int, fallback_value=None)
                .kwargs()
            )
        self.assertEqual(cm.exception.col, "id")
        self.assertEqual(cm.exception.value, "foo")
