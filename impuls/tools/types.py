import sys
import typing
from typing import TYPE_CHECKING, Type, TypeVar, Union

if TYPE_CHECKING:
    from typing_extensions import Self
else:
    Self = TypeVar("Self")


if sys.version_info >= (3, 10):
    from types import UnionType
else:
    # Use a sentinel object instead of UnionType, as it does not exist before 3.10
    # The sentinel object is used so that `is` with any other object returns False.
    UnionType = object()


SQLNativeType = None | int | float | str
T = TypeVar("T")


def identity(x: T) -> T:
    return x


def union_to_tuple_of_types(tp: Type[T]) -> tuple[Type[T], ...]:
    """union_to_tuple_of_types tp if it's a non-type-hint type,
    the type arguments of Union (and therefore also Optional) and UnionType;
    and raises TypeError for any other type hints.

    >>> union_to_tuple_of_types(int)
    (<class 'int'>,)
    >>> union_to_tuple_of_types(Union[int, str])
    (<class 'int'>, <class 'str'>)
    """
    origin = typing.get_origin(tp)
    if origin is None:
        return (tp,)
    elif origin is Union or origin is UnionType:
        return typing.get_args(tp)
    else:
        raise TypeError(f"{tp} is neither a non-type-hint type, nor a union")
