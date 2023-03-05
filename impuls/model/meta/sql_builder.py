from typing import Any, Callable, Sequence, Type, TypeVar

from ...tools.types import Self, SQLNativeType, identity

SQL_T = TypeVar("SQL_T", bound=SQLNativeType)


class DataclassSQLBuilder:
    def __init__(self, row: Sequence[SQLNativeType]) -> None:
        self.row = row
        self.i = 0
        self.fields: dict[str, Any] = {}

    def kwargs(self) -> dict[str, Any]:
        if self.i < len(self.row):
            raise RuntimeError(f"Too few fields, expected {len(self.row)}, got {self.i}")
        return self.fields

    def field(
        self: Self,
        field: str,
        incoming_type: Type[SQL_T],
        converter: Callable[[SQL_T], Any] = identity,
        nullable: bool = False,
    ) -> Self:
        # Retrieve the current argument
        if self.i >= len(self.row):
            raise RuntimeError(f"Too many fields, expected {len(self.row)}")
        v = self.row[self.i]

        # Check if type matches
        # FIXME: Support Optional[...] types.
        if nullable and v is None:
            pass  # Bypass instance check if nulls are allowed and v is null
        elif not isinstance(v, incoming_type):
            expected_type = (
                f"Optional[{incoming_type.__name__}]" if nullable else incoming_type.__name__
            )

            raise TypeError(
                f"Wrong type from DB for field {field}: expected {expected_type}"
                f", got {type(v).__name__}"
            )

        # Set the field
        self.fields[field] = converter(v) if v is not None else v
        self.i += 1
        return self
