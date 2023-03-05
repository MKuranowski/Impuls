from typing import Any, Callable, Mapping

from ...errors import DataError
from ...tools.types import Self, identity


class MissingGTFSColumn(DataError):
    def __init__(self, col: str) -> None:
        super().__init__(f"Missing GTFS column: {col}")
        self.col = col


class InvalidGTFSCellValue(DataError):
    def __init__(self, col: str, value: str) -> None:
        super().__init__(f"Invalid value for GTFS column {col}: {value!r}")
        self.col = col
        self.value = value


class DataclassGTFSBuilder:
    def __init__(self, row: Mapping[str, str]) -> None:
        self.row = row
        self.fields: dict[str, Any] = {}

    def kwargs(self) -> dict[str, Any]:
        return self.fields

    def field(
        self: Self,
        field: str,
        gtfs_col: str,
        converter: Callable[[str], Any] = identity,
        fallback_value: Any = None,
    ) -> Self:
        raw_value = self.row.get(gtfs_col)
        if raw_value is None and fallback_value is None:
            raise MissingGTFSColumn(gtfs_col)
        elif raw_value is None:
            self.fields[field] = fallback_value
        else:
            try:
                self.fields[field] = converter(raw_value)
            except ValueError as e:
                raise InvalidGTFSCellValue(gtfs_col, raw_value) from e
        return self
