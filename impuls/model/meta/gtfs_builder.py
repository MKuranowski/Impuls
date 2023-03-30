from functools import partial
from typing import Any, Callable, Final, Mapping, Optional

from ...errors import DataError
from ...tools.types import Self


class MissingGTFSColumn(DataError):
    """MissingGTFSColumn is raised by DataclassGTFSBuilder
    when an required GTFS column is missing."""

    def __init__(self, col: str) -> None:
        super().__init__(f"Missing GTFS column: {col}")
        self.col = col


class InvalidGTFSCellValue(DataError):
    """InvalidGTFSCellValue is raised by DataclassGTFSBuilder
    when a string-to-value converter raises ValueError."""

    def __init__(self, col: str, value: str) -> None:
        super().__init__(f"Invalid value for GTFS column {col}: {value!r}")
        self.col = col
        self.value = value


class DataclassGTFSBuilder:
    """DataclassGTFSBuilder prepares keyword arguments from a row returned by csv.DictReader"""

    NO_FALLBACK_VALUE: Final[object] = object()
    """NO_FALLBACK_VALUE is a sentinel object used by DataclassGTFSBuilder.field
    to denote that there is no fallback value provided.
    This allows None to be used as a fallback value."""

    def __init__(self, row: Mapping[str, str]) -> None:
        self.row = row
        self.fields: dict[str, Any] = {}

    def kwargs(self) -> dict[str, Any]:
        """kwargs returns the prepared fields in a dictionary."""
        return self.fields

    def field(
        self: Self,
        field: str,
        gtfs_col: str,
        converter: Optional[Callable[[str], Any]] = None,
        fallback_value: Any = NO_FALLBACK_VALUE,
    ) -> Self:
        """field consumes `row[gtfs_col]`, transforms it and adds it in the kwargs
        under the `field` name.

        `converter`, if not None, will be called to convert the incoming string
        to a target type. converter must raise ValueError on invalid inputs.

        If `fallback_value` is DataclassGTFSBuilder.NO_FALLBACK_VALUE, then
        MissingGTFSColumn will be raised if gtfs_col is not in the provided row.

        Otherwise, if the gtfs_col is not in the provided row, `kwargs[field]` will be set
        to the `fallback_value` directly, bypassing the converter.
        """
        raw_value = self.row.get(gtfs_col)
        if raw_value is None and fallback_value is self.NO_FALLBACK_VALUE:
            raise MissingGTFSColumn(gtfs_col)
        elif raw_value is None:
            self.fields[field] = fallback_value
        else:
            try:
                self.fields[field] = converter(raw_value) if converter is not None else raw_value
            except ValueError as e:
                raise InvalidGTFSCellValue(gtfs_col, raw_value) from e
        return self


def to_optional(x: Any) -> str:
    """to_optional return str(x), unless x is None - in that case returns an empty string

    >>> to_optional("Hello")
    'Hello'
    >>> to_optional(1)
    '1'
    >>> to_optional(None)
    ''
    """
    return "" if x is None else str(x)


def to_bool(x: str, allow_empty: bool = False) -> bool:
    """Tries to parse a required GTFS boolean value.

    >>> to_bool("0")
    False
    >>> to_bool("1")
    True
    >>> to_bool("")
    Traceback (most recent call last):
    ...
    ValueError: Invalid GTFS value: '' (expected '0' or '1')
    >>> to_bool("", allow_empty=True)
    False
    >>> to_bool("foo")
    Traceback (most recent call last):
    ...
    ValueError: Invalid GTFS value: 'foo' (expected '0' or '1')
    """
    if x == "0":
        return False
    elif x == "1":
        return True
    elif x == "" and allow_empty:
        return False
    else:
        raise ValueError(f"Invalid GTFS value: {x!r} (expected '0' or '1')")


to_bool_allow_empty: Callable[[str], bool] = partial(to_bool, allow_empty=True)
"""to_bool_allow_empty is an alias to call to_bool(..., allow_empty=True)"""


def from_bool(x: bool) -> str:
    """Converts a boolean to a required GTFS boolean field.

    >>> from_bool(True)
    '1'
    >>> from_bool(False)
    '0'
    """
    if x is True:
        return "1"
    else:
        return "0"


def to_optional_bool_empty_none(x: str) -> Optional[bool]:
    """Tries to parse an optional GTFS boolean value;
    where the empty string represents None; such as trips.exceptional.

    >>> to_optional_bool_empty_none("0")
    False
    >>> to_optional_bool_empty_none("1")
    True
    >>> to_optional_bool_empty_none("")
    >>> to_optional_bool_empty_none("foo")
    Traceback (most recent call last):
    ...
    ValueError: Invalid GTFS value: 'foo' (expected '', '0' or '1')
    """
    if x == "":
        return None
    elif x == "0":
        return False
    elif x == "1":
        return True
    else:
        raise ValueError(f"Invalid GTFS value: {x!r} (expected '', '0' or '1')")


def from_optional_bool_empty_none(x: Optional[bool]) -> str:
    """Converts a boolean to an optional GTFS boolean field,
    where the empty string represents None; such as trips.exceptional.

    >>> from_optional_bool_empty_none(True)
    '1'
    >>> from_optional_bool_empty_none(False)
    '0'
    >>> from_optional_bool_empty_none(None)
    ''
    """
    if x is True:
        return "1"
    elif x is False:
        return "0"
    else:
        return ""


def to_optional_bool_zero_none(x: str) -> Optional[bool]:
    """Tries to parse an optional GTFS boolean value;
    where '0' or '' represents None; such as trips.wheelchair_accessible.

    >>> to_optional_bool_zero_none("0")
    >>> to_optional_bool_zero_none("1")
    True
    >>> to_optional_bool_zero_none("2")
    False
    >>> to_optional_bool_zero_none("")
    >>> to_optional_bool_zero_none("foo")
    Traceback (most recent call last):
    ...
    ValueError: Invalid GTFS value: 'foo' (expected '', '0', '1' or '2')
    """
    if x == "" or x == "0":
        return None
    elif x == "1":
        return True
    elif x == "2":
        return False
    else:
        raise ValueError(f"Invalid GTFS value: {x!r} (expected '', '0', '1' or '2')")


def from_optional_bool_zero_none(x: Optional[bool]) -> str:
    """Converts a boolean to an optional GTFS boolean field,
    where '0' represents None; such as trips.wheelchair_accessible.

    >>> from_optional_bool_zero_none(True)
    '1'
    >>> from_optional_bool_zero_none(False)
    '2'
    >>> from_optional_bool_zero_none(None)
    '0'
    """
    if x is True:
        return "1"
    elif x is False:
        return "2"
    else:
        return "0"
