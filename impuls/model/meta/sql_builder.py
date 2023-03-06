import sys
import typing
from typing import Any, Callable, Optional, Sequence, Type, TypeVar, Union

from ...tools.types import Self, SQLNativeType, T

SQL_T = TypeVar("SQL_T", bound=SQLNativeType)


class DataclassSQLBuilder:
    """DataclassSQLBuilder prepares keyword arguments from a row returned by sqlite.

    The number of fields set must be the same as the number of elements in the returned row,
    otherwise RuntimeError is thrown.
    """

    def __init__(self, row: Sequence[SQLNativeType]) -> None:
        self.row = row
        self.i = 0
        self.fields: dict[str, Any] = {}

    def kwargs(self) -> dict[str, Any]:
        """kwargs returns the prepared fields in a dictionary."""
        if self.i < len(self.row):
            raise RuntimeError(f"Too few fields, expected {len(self.row)}, got {self.i}")
        return self.fields

    def field(
        self: Self,
        field: str,
        incoming_type: Type[SQL_T],
        converter: Optional[Callable[[SQL_T], Any]] = None,
        nullable: bool = False,
    ) -> Self:
        """field consumes next element from the SQL row and adds it the the kwargs under the
        `field` name.

        The `incoming_type` is passed to isinstance; and (to maintain 3.9 compatibility)
        passing Union[...] or Optional[...] is not allowed.

        If `converter` is not None, `converter(incoming_value)` will be returned
        in the keyword arguments instead of the incoming value.

        If `nullable` is set to True, and the incoming value is `None`, `None` will be returned,
        bypassing `converter` and the `isinstance` check.

        Comparison of different semantics between nullable and incoming_type=Optional[...]

        | incoming type | converter         | nullable | saved type          |
        |---------------|-------------------|----------|---------------------|
        | T             | None              | False    | T                   |
        | T             | T -> U            | False    | U                   |
        | T \\| None    | None              | False    | T \\| None          |
        | T \\| None    | (T \\| None) -> U | False    | U                   |
        | T             | None              | True     | T \\| None          |
        | T             | T -> U            | True     | U \\| None          |
        | T \\| None    | T -> U            | True     | U \\| None          |

        """
        # Retrieve the current argument
        if self.i >= len(self.row):
            raise RuntimeError(f"Too many fields, expected {len(self.row)}")
        value = self.row[self.i]

        # Special case for nullable columns
        if nullable and value is None:
            self.fields[field] = None
            self.i += 1
            return self

        # Type-check the incoming value
        allowed_types = _union_to_tuple_of_types(incoming_type)
        if not isinstance(value, allowed_types):
            got_type = type(value).__name__
            expected_type = str(tuple(t.__name__ for t in allowed_types))  # type: ignore
            raise TypeError(f"{field}: got {got_type}; expected {expected_type}")

        # Save the value; passing it through the converter
        self.fields[field] = converter(value) if converter is not None else value
        self.i += 1
        return self


if sys.version_info >= (3, 10):
    from types import UnionType
else:
    UnionType = object()


def _union_to_tuple_of_types(tp: Type[T]) -> tuple[Type[T], ...]:
    origin = typing.get_origin(tp)
    if origin is None:
        return (tp,)
    elif origin is Union or origin is UnionType:
        return typing.get_args(tp)
    else:
        raise TypeError(f"{tp} is neither a type, nor a union")
