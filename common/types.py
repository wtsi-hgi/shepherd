"""
Copyright (c) 2019 Genome Research Limited

Author: Christopher Harrison <ch12@sanger.ac.uk>

This program is free software: you can redistribute it and/or modify it
under the terms of the GNU General Public License as published by the
Free Software Foundation, either version 3 of the License, or (at your
option) any later version.

This program is distributed in the hope that it will be useful, but
WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
Public License for more details.

You should have received a copy of the GNU General Public License along
with this program. If not, see https://www.gnu.org/licenses/
"""

# Make Python's type definitions available
from numbers import *
from pathlib import *
from types import *
from typing import *

from .time import datetime as DateTime, \
                  timedelta as TimeDelta


# Generic identifier
Identifier = TypeVar("Identifier")


_T = TypeVar("_T")

class Carrier(Generic[_T]):
    """ Generic carrier type """
    # NOTE While this is a container, in the semantic sense, it is
    # different to Python's notion of a container (per typing.Container)
    _payload:_T

    @property
    def payload(self) -> _T:
        return self._payload

    @payload.setter
    def payload(self, value:_T) -> None:
        self._payload = value


class Named:
    """ Named object mixin """
    _name:str

    @property
    def name(self) -> str:
        """ Return the name of the object """
        return self._name
