from dataclasses import dataclass, field
from enum import IntEnum
from typing import Mapping, Optional, Sequence
from typing import Type as TypeOf
from typing import cast, final

from typing_extensions import LiteralString

from ..tools.types import Self, SQLNativeType
from .impuls_base import ImpulsBase
from .utility_types import Date, Maybe, TimePoint


@final
@dataclass(unsafe_hash=True)
class Attribution(ImpulsBase):
    id: str = field(compare=True)
    organization_name: str = field(compare=False)
    is_producer: bool = field(default=False, compare=False, repr=False)
    is_operator: bool = field(default=False, compare=False, repr=False)
    is_authority: bool = field(default=False, compare=False, repr=False)
    is_data_source: bool = field(default=False, compare=False, repr=False)
    url: str = field(default="", compare=False, repr=False)
    email: str = field(default="", compare=False, repr=False)
    phone: str = field(default="", compare=False, repr=False)
