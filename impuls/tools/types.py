from typing import TYPE_CHECKING, TypeVar

if TYPE_CHECKING:
    from typing_extensions import Self
else:
    Self = TypeVar("Self")

SQLNativeType = None | int | float | str
T = TypeVar("T")


def identity(x: T) -> T:
    return x
