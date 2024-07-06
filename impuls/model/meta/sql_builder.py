from typing import Any, Callable, Optional, Sequence, Type, TypeVar

from ...tools.types import Self, SQLNativeType

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

    def _get_value(self) -> SQLNativeType:
        if self.i >= len(self.row):
            raise RuntimeError(f"Too many fields, expected {len(self.row)}")
        return self.row[self.i]

    def _save_field(self, field: str, value: Any) -> None:
        self.fields[field] = value
        self.i += 1

    def field(
        self: Self,
        field: str,
        incoming_type: Type[SQL_T],
        converter: Optional[Callable[[SQL_T], Any]] = None,
    ) -> Self:
        """field consumes next element from the SQL row and adds it to the kwargs under the
        ``field`` name.

        ``incoming_type`` is used to type-check the incoming field - the incoming value
        must be an instance of ``incoming_type``.

        ``converter``, if present will be applied to the incoming value. This can be used
        to change between SQL and Impuls types. For convenience, if ``incoming_type`` is ``bool``,
        a converter is automatically provided.

        See :py:meth:`nullable_field` and :py:meth:`optional_field` if the incoming value may be
        NULL.
        """
        # Retrieve the current argument
        value = self._get_value()

        # Special case for bool
        if incoming_type is bool:
            incoming_type = int  # type: ignore
            converter = bool

        # Type check value
        if not isinstance(value, incoming_type):
            raise TypeError(
                f"{field}: got {type(value).__name__}, expected {incoming_type.__name__}"
            )

        # Save the field
        self._save_field(field, converter(value) if converter else value)
        return self

    def nullable_field(
        self,
        field: str,
        incoming_type: Type[SQL_T],
        converter: Optional[Callable[[SQL_T], Any]] = None,
    ) -> Self:
        """nullable_field consumes next element from the SQL row and adds it to the kwargs
        under the ``field`` name.

        ``incoming_type`` is used to type-check the incoming field - the incoming value
        must be an instance of ``incoming_type``, or be None.

        ``converter``, if present will be applied to the incoming value, if that is not NULL.
        For convenience, if ``incoming_type`` is ``bool``, a converter is automatically provided.

        NULL values bypass the converter (in contrast with :py:meth:`optional_field`) and are
        saved directly as ``None``.
        """
        # Retrieve the current argument
        value = self._get_value()

        # Allow NULLs
        if value is None:
            self._save_field(field, None)
            return self

        # Special case for bool
        if incoming_type is bool:
            incoming_type = int  # type: ignore
            converter = bool

        # Type check value
        if not isinstance(value, incoming_type):
            raise TypeError(
                f"{field}: got {type(value).__name__}, expected {incoming_type.__name__}"
            )

        # Save the field
        self._save_field(field, converter(value) if converter else value)
        return self

    def optional_field(
        self,
        field: str,
        incoming_type: Type[SQL_T],
        converter: Callable[[Optional[SQL_T]], Any],
    ) -> Self:
        """optional_field consumes next element from the SQL row and adds it to the kwargs
        under the ``field`` name.

        ``incoming_type`` is used to type-check the incoming field - the incoming value
        must be an instance of ``incoming_type``, or be ``None``.

        ``converter`` is always applied to the incoming value, regardless if it's ``None`` or not.
        If you find ``converter`` optional, use :py:meth:`nullable_field` instead.

        NULL values are passed the converter (in contrast with :py:meth:`nullable_field`).
        """
        # Retrieve the current argument
        value = self._get_value()

        # Type check value
        if value is not None and not isinstance(value, incoming_type):
            raise TypeError(
                f"{field}: got {type(value).__name__}, expected Optional[{incoming_type.__name__}]"
            )

        # Save the field
        self._save_field(field, converter(value))
        return self
