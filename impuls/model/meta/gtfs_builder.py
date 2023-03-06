from typing import Any, Callable, Mapping, Optional

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


def unmarshall_bool(x: str) -> bool:
    """Tries to parse a required GTFS boolean value.

    >>> unmarshall_bool("0")
    False
    >>> unmarshall_bool("1")
    True
    >>> unmarshall_bool("")
    Traceback (most recent call last):
    ...
    ValueError: Invalid GTFS value: '' (expected '0' or '1')
    >>> unmarshall_bool("foo")
    Traceback (most recent call last):
    ...
    ValueError: Invalid GTFS value: 'foo' (expected '0' or '1')
    """
    if x == "0":
        return False
    elif x == "1":
        return True
    else:
        raise ValueError(f"Invalid GTFS value: {x!r} (expected '0' or '1')")


def marshall_bool(x: bool) -> str:
    """Converts a boolean to a required GTFS boolean field.

    >>> marshall_bool(True)
    '1'
    >>> marshall_bool(False)
    '0'
    """
    if x is True:
        return "1"
    else:
        return "0"


def unmarshall_optional_bool_empty_none(x: str) -> Optional[bool]:
    """Tries to parse an optional GTFS boolean value;
    where the empty string represents None; such as trips.exceptional.

    >>> unmarshall_bool("0")
    False
    >>> unmarshall_bool("1")
    True
    >>> unmarshall_bool("")
    None
    >>> unmarshall_bool("foo")
    Traceback (most recent call last):
    ...
    ValueError: Invalid GTFS value: 'foo' (expected '0' or '1')
    """
    if x == "":
        return None
    elif x == "0":
        return False
    elif x == "1":
        return True
    else:
        raise ValueError(f"Invalid GTFS value: {x!r} (expected '', '0' or '1')")


def marshall_optional_bool_empty_none(x: Optional[bool]) -> str:
    """Converts a boolean to an optional GTFS boolean field,
    where the empty string represents None; such as trips.exceptional.

    >>> marshall_optional_bool_empty_none(True)
    '1'
    >>> marshall_optional_bool_empty_none(False)
    '0'
    >>> marshall_optional_bool_empty_none(None)
    ''
    """
    if x is True:
        return "1"
    elif x is False:
        return "0"
    else:
        return ""


def unmarshall_optional_bool_zero_none(x: str) -> Optional[bool]:
    """Tries to parse an optional GTFS boolean value;
    where '0' or '' represents None; such as trips.wheelchair_accessible.

    >>> unmarshall_bool("0")
    None
    >>> unmarshall_bool("1")
    True
    >>> unmarshall_bool("2")
    False
    >>> unmarshall_bool("")
    None
    >>> unmarshall_bool("foo")
    Traceback (most recent call last):
    ...
    ValueError: Invalid GTFS value: 'foo' (expected '0' or '1')
    """
    if x == "" or x == "0":
        return None
    elif x == "1":
        return True
    elif x == "2":
        return False
    else:
        raise ValueError(f"Invalid GTFS value: {x!r} (expected '', '0', '1' or '2')")


def marshall_optional_bool_zero_none(x: Optional[bool]) -> str:
    """Converts a boolean to an optional GTFS boolean field,
    where '0' represents None; such as trips.wheelchair_accessible.

    >>> marshall_optional_bool_empty_none(True)
    '1'
    >>> marshall_optional_bool_empty_none(False)
    '2'
    >>> marshall_optional_bool_empty_none(None)
    '0'
    """
    if x is True:
        return "1"
    elif x is False:
        return "2"
    else:
        return "0"
