import dataclasses
from enum import IntEnum
from types import NoneType
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Mapping,
    NamedTuple,
    Sequence,
    Type,
    TypedDict,
    TypeVar,
    Union,
)
from typing import get_args as get_type_args
from typing import get_origin as get_type_origin

from ..tools.strings import camel_to_snake
from .utility_types import Date, TimePoint

if TYPE_CHECKING:
    from typing_extensions import Self
else:
    Self = TypeVar("Self")


_T = TypeVar("_T")
_IB = TypeVar("_IB", bound="ImpulsBase")
_IntEnum = TypeVar("_IntEnum", bound=IntEnum)


SQLSupportedType = None | int | float | str


class SQLFieldDescription(NamedTuple):
    field_name: str

    column_name: str
    sql_type: str
    primary_key: bool
    foreign_key: str
    not_null: bool
    indexed: bool

    marshall: Callable[[Any], SQLSupportedType]
    unmarshall: Callable[[SQLSupportedType], Any]


class GTFSFieldDescription(NamedTuple):
    field_name: str

    column_name: str
    is_required: bool

    marshall: Callable[[Any], str]
    unmarshall: Callable[[str], Any]


class TypeMetadata(TypedDict, total=False):
    # Forces a specific table name in the database and GTFS. If empty,
    # defaults to class name converted to snake case and 's' added at the end
    table_name: str

    # Forces a specific table name in the GTFS. Defaults to the table_name.
    gtfs_table_name: str


class FieldMetadata(TypedDict, total=False):
    # Whether the field should be a part of DB's primary key
    primary_key: bool

    # Whether the field should be a foreign key
    # if yes, must be string of format "table_name(column_name)"
    foreign_key: str

    # Whether the field in GTFS is **not** prefixed by the entity name.
    # Defaults to true on foreign keys, false otherwise.
    gtfs_no_entity_prefix: bool

    # Final override of the gtfs column name.
    # The default is the entity name joined with field name (in snake_case),
    # or only the field name if gtfs_no_entity_prefix is set.
    gtfs_column_name: str

    # Whether to explicitly index this field in the database.
    # Defaults to true for primary and foreign keys; false for other fields.
    index: bool


def _identity(x: _T) -> _T:
    return x


_sql_marshallers: dict[type, Callable[[Any], SQLSupportedType]] = {
    NoneType: _identity,
    str: lambda x: x or None,  # coalesce empty strings into None
    int: _identity,
    float: _identity,
    bool: int,
    TimePoint: lambda x: int(x.total_seconds()),
    Date: lambda x: x.strftime("%Y-%m-%d"),
    # IntEnum - special case - added by the impuls_base decorator
}

_sql_unmarshallers: dict[type, Callable[[SQLSupportedType], Any]] = {
    NoneType: _identity,
    str: lambda x: x or "",  # coalesce NULL into ""
    int: _identity,
    float: _identity,
    bool: bool,
    TimePoint: lambda x: TimePoint(seconds=x),  # type: ignore
    Date: lambda x: Date(int(x[0:4]), int(x[5:7]), int(x[8:10])),  # type: ignore
    # IntEnum - special case - added by the impuls_base decorator
}

_gtfs_marshallers: dict[type, Callable[[Any], str]] = {
    NoneType: lambda _: "",
    str: _identity,
    int: str,
    float: str,
    bool: lambda x: "1" if x else "0",
    TimePoint: str,
    Date: lambda x: x.strftime("%Y%m%d"),
    # IntEnum - special case - added by the impuls_base decorator
}

_gtfs_unmarshallers: dict[type, Callable[[str], Any]] = {
    NoneType: lambda _: None,
    str: _identity,
    int: int,
    float: float,
    bool: lambda x: x == "1",
    TimePoint: lambda x: TimePoint.from_str(x),
    Date: lambda x: Date(int(x[0:4]), int(x[4:6]), int(x[6:8])),
    # IntEnum - special case - added by the impuls_base decorator
}


class ImpulsBase:
    def __init__(self, *args: Sequence[Any], **kwargs: Mapping[str, Any]) -> None:
        raise NotImplementedError("ImpulsBase protocol not implemented")

    # GTFS Interface

    @classmethod
    @property
    def _gtfs_table_name(cls) -> str:
        raise NotImplementedError("ImpulsBase protocol not implemented")

    @classmethod
    @property
    def _gtfs_fields(cls) -> dict[str, GTFSFieldDescription]:
        raise NotImplementedError("ImpulsBase protocol not implemented")

    @classmethod
    def _gtfs_unmarshall(cls: Type[Self], row: Mapping[str, str]) -> Self:
        return cls(
            **{
                f.field_name: f.unmarshall(row.get(f.column_name, ""))
                for f in cls._gtfs_fields.values()
            }
        )

    def _gtfs_marshall(self) -> Mapping[str, str]:
        return {
            f.column_name: f.marshall(getattr(self, f.field_name))
            for f in self._gtfs_fields.values()
        }

    # SQLite Interface

    @classmethod
    @property
    def _sql_table_name(cls) -> str:
        raise NotImplementedError("ImpulsBase protocol not implemented")

    @classmethod
    @property
    def _sql_fields(cls) -> dict[str, SQLFieldDescription]:
        raise NotImplementedError("ImpulsBase protocol not implemented")

    @classmethod
    @property
    def _sql_primary_key_columns(cls) -> dict[str, SQLFieldDescription]:
        raise NotImplementedError("ImpulsBase protocol not implemented")

    def _sql_primary_key(self) -> tuple[SQLSupportedType, ...]:
        return tuple(
            f.marshall(getattr(self, f.field_name)) for f in self._sql_primary_key_columns.values()
        )

    @classmethod
    def _sql_unmarshall(cls: Type[Self], row: tuple[SQLSupportedType, ...]) -> Self:
        return cls(
            **{f.field_name: f.unmarshall(elem) for elem, f in zip(row, cls._sql_fields.values())}
        )

    def _sql_marshall(self) -> tuple[SQLSupportedType, ...]:
        return tuple(f.marshall(getattr(self, f.field_name)) for f in self._sql_fields.values())


def impuls_base(typ: Type[_IB]) -> Type[_IB]:
    """Wrapper that implements the ImpulsBase protocol for dataclasses using
    field and type metadata.

    Type metadata must be provided via a class variable named `_metadata`.
    Field metadata must be provided on dataclass.Field objects.
    """

    typ_meta: TypeMetadata = getattr(typ, "_metadata", {})  # type: ignore
    entity_snake_case = camel_to_snake(typ.__name__)

    # Figure out the table name
    table_name = typ_meta.get("table_name", entity_snake_case + "s")
    gtfs_table_name = typ_meta.get("gtfs_table_name", table_name)

    setattr(typ, "_sql_table_name", table_name)
    setattr(typ, "_gtfs_table_name", gtfs_table_name)

    # Process the fields
    gtfs_fields: dict[str, GTFSFieldDescription] = {}
    sql_fields: dict[str, SQLFieldDescription] = {}

    for field in dataclasses.fields(typ):
        # Extract FieldMetadata
        if TYPE_CHECKING:
            assert isinstance(field.metadata, FieldMetadata)
        primary_key = field.metadata.get("primary_key", False)
        foreign_key = field.metadata.get("foreign_key", "")
        gtfs_no_entity_prefix = field.metadata.get("gtfs_no_entity_prefix", False)
        gtfs_column_name = field.metadata.get("gtfs_column_name") or _generate_gtfs_column_name(
            field.name, entity_snake_case, gtfs_no_entity_prefix
        )
        index = field.metadata.get("index", False) or bool(foreign_key)

        # Rules for dealing with SQL null:
        # - `Optional[...]` can be NULL
        # - `str` (non-primary keys) can be NULL.
        #    Empty strings are converted to NULL on the fly.
        # - Everything else is non-NULL

        # Rules for dealing with GTFS optional columns:
        # - `Optional[...]` are optional columns: None ↔ "", rest as usual
        # - Every field with `default` are optional columns: default → "", rest as usual
        # - Everything else is not an optional column.

        field_type, maybe_none = _concrete_type(field.type)
        sql_optional = maybe_none or (field.type is str and not primary_key)
        gtfs_optional = maybe_none or field.default != dataclasses.MISSING

        if maybe_none and field.default is not None:
            raise ValueError(
                f"{typ.__name__}.{field.name} is of type Optional[T], "
                f"but its default value is not None (got {field.default!r})"
            )

        # Declare global marshallers and unmarshallers for enums as we go

        if issubclass(field_type, IntEnum):
            _sql_marshallers[field_type] = int
            _sql_unmarshallers[field_type] = field_type  # type: ignore
            _gtfs_marshallers[field_type] = lambda x: str(int(x))
            _gtfs_unmarshallers[field_type] = _str_to_enum_converter(field_type)

        # Set the fields

        sql_fields[field.name] = SQLFieldDescription(
            field_name=field.name,
            column_name=field.name,
            sql_type=_sql_type(field_type),
            primary_key=primary_key,
            foreign_key=foreign_key,
            not_null=not sql_optional,
            indexed=index,
            marshall=_nice_sql_marshaller(field_type, maybe_none, typ.__name__, field.name),
            unmarshall=_nice_sql_unmarshaller(field_type, sql_optional, typ.__name__, field.name),
        )

        gtfs_fields[field.name] = GTFSFieldDescription(
            field_name=field.name,
            column_name=gtfs_column_name,
            is_required=not gtfs_optional,
            marshall=_nice_gtfs_marshaller(field_type, maybe_none, typ.__name__, field.name),
            unmarshall=_nice_gtfs_unmarshaller(
                field_type, field.default, typ.__name__, field.name
            ),
        )

    setattr(typ, "_gtfs_fields", gtfs_fields)
    setattr(typ, "_sql_fields", sql_fields)
    setattr(
        typ, "_sql_primary_key_columns", {k: v for k, v in sql_fields.items() if v.primary_key}
    )

    return typ


def _concrete_type(annotation: Any) -> tuple[type, bool]:
    """
    Destructs `Optional` (unions with None) into the concrete type
    and a boolean informing if it's a

    >>> from typing import Optional
    >>> _concrete_type(str)
    (<class 'str'>, False)
    >>> _concrete_type(Optional[str])
    (<class 'str'>, True)
    """
    if isinstance(annotation, type):
        return annotation, False

    elif get_type_origin(annotation) is Union:
        args = get_type_args(annotation)
        if args[0] is NoneType:
            return args[1], True
        elif args[1] is NoneType:
            return args[0], True
        else:
            raise ValueError(f"Unions must be (T, None); got {args}")

    else:
        raise ValueError(f"Argument must be a concrete type or `T | None` union; got {annotation}")


def _generate_gtfs_column_name(
    field_name: str, entity_snake_case: str, gtfs_no_entity_prefix: bool = True
) -> str:
    """
    Generates a name for the GTFS column, depending on the `gtfs_no_entity_prefix` param

    >>> _generate_gtfs_column_name("id", "route", gtfs_no_entity_prefix=False)
    'route_id'
    >>> _generate_gtfs_column_name("code", "stop", gtfs_no_entity_prefix=False)
    'stop_code'
    >>> _generate_gtfs_column_name("stop_sequence", "stop_time", gtfs_no_entity_prefix=True)
    'stop_sequence'
    >>> _generate_gtfs_column_name("date", "calendar_exception", gtfs_no_entity_prefix=True)
    'date'
    """
    return field_name if gtfs_no_entity_prefix else f"{entity_snake_case}_{field_name}"


def _sql_type(typ: type) -> str:
    """
    Finds a suitable SQL type for a given type used by Impuls model classes.

    >>> _sql_type(IntEnum)
    'INTEGER'
    >>> _sql_type(str)
    'TEXT'
    >>> _sql_type(float)
    'REAL'
    >>> _sql_type(complex)
    Traceback (most recent call last):
    ...
    ValueError: Unsupported type for storing in SQLite3 database: <class 'complex'>
    """
    if issubclass(typ, IntEnum):
        return "INTEGER"

    sql_type = {
        str: "TEXT",
        int: "INTEGER",
        float: "REAL",
        bool: "INTEGER",
        TimePoint: "INTEGER",
        Date: "TEXT",
        IntEnum: "INTEGER",
    }.get(
        typ  # type: ignore
    )

    if not sql_type:
        raise ValueError(f"Unsupported type for storing in SQLite3 database: {typ}")

    return sql_type


def _nice_sql_marshaller(
    typ: type, maybe_none: bool, class_name: str, field_name: str
) -> Callable[[Any], SQLSupportedType]:
    """Returns a function that marshalls entities of type `typ` into
    their SQL counterparts.

    The returned function will handle `None` inputs if maybe_none is True.

    The returned function will also have slightly tweaked __name__ and __qualname__
    for nicer error reporting.
    """
    # Find the function converting type
    converter: Callable[[Any], SQLSupportedType] = _sql_marshallers[typ]
    if maybe_none:
        converter = lambda x: None if x is None else converter(x)

    # Create a nice function that catches creates better errors
    def f(x: Any) -> SQLSupportedType:
        try:
            return converter(x)
        except ValueError as e:
            raise ValueError(f"Invalid value for {class_name}.{field_name}: {x!r}") from e

    # Prettify its attributes
    f.__name__ = f"{field_name}.sql_marshall"
    f.__qualname__ = f"{class_name}.{field_name}.sql_marshall"

    return f


def _nice_sql_unmarshaller(
    typ: type, sql_optional: bool, class_name: str, field_name: str
) -> Callable[[SQLSupportedType], Any]:
    """Returns a function that unmarshalls entities to type `typ` from
    their SQL counterparts.

    The returned function will handle `NULL` inputs if sql_optional is True.

    The returned function will also have slightly tweaked __name__ and __qualname__
    for nicer error reporting.
    """
    # Find the function converting type
    converter: Callable[[SQLSupportedType], Any] = _sql_unmarshallers[typ]
    if sql_optional and typ is not str:
        converter = lambda x: None if x is None else converter(x)

    # Create a nice function that catches creates better errors
    def f(x: SQLSupportedType) -> Any:
        try:
            return converter(x)
        except ValueError as e:
            raise ValueError(f"Invalid value for {class_name}.{field_name}: {x!r}") from e

    # Prettify its attributes
    f.__name__ = f"{field_name}.sql_unmarshall"
    f.__qualname__ = f"{class_name}.{field_name}.sql_unmarshall"

    return f


def _nice_gtfs_marshaller(
    typ: type, maybe_none: bool, class_name: str, field_name: str
) -> Callable[[Any], str]:
    """Returns a function that marshalls entities of type `typ` into
    their GTFS string representation.

    The returned function will handle `None` inputs if maybe_none is True.

    The returned function will also have slightly tweaked __name__ and __qualname__
    for nicer error reporting.
    """
    # Find the function converting type
    converter: Callable[[Any], str] = _gtfs_marshallers[typ]
    if maybe_none:
        converter = lambda x: "" if x is None else converter(x)

    # Create a nice function that catches creates better errors
    def f(x: Any) -> str:
        try:
            return converter(x)
        except ValueError as e:
            raise ValueError(f"Invalid value for {class_name}.{field_name}: {x!r}") from e

    # Prettify its attributes
    f.__name__ = f"{field_name}.gtfs_marshall"
    f.__qualname__ = f"{class_name}.{field_name}.gtfs_marshall"

    return f


def _nice_gtfs_unmarshaller(
    typ: type, default: Any, class_name: str, field_name: str
) -> Callable[[str], Any]:
    """Returns a function that unmarshalls entities to type `typ` from
    their GTFS string representation.

    The returned function will handle `""` inputs if default is not dataclasses.MISSING sentinel
    object.

    The returned function will also have slightly tweaked __name__ and __qualname__
    for nicer error reporting.
    """
    # Find the function converting type
    converter: Callable[[str], Any] = _gtfs_unmarshallers[typ]
    if default is not dataclasses.MISSING:
        converter = lambda x: default if x == "" else converter(x)

    # Create a nice function that catches creates better errors
    def f(x: str) -> Any:
        try:
            return converter(x)
        except ValueError as e:
            raise ValueError(f"Invalid value for {class_name}.{field_name}: {x!r}") from e

    # Prettify its attributes
    f.__name__ = f"{field_name}.gtfs_unmarshall"
    f.__qualname__ = f"{class_name}.{field_name}.gtfs_unmarshall"

    return f


def _str_to_enum_converter(t: Type[_IntEnum]) -> Callable[[str], _IntEnum]:
    """
    Returns a function that takes int-containing strings and converts them into
    instances of provided IntEnum.

    Useful when `lambda x: t(int(x))` can't be used in-place due to
    closure overwrites.
    """
    return lambda x: t(int(x))  # type: ignore
