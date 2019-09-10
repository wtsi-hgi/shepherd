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

from abc import ABCMeta, abstractmethod

from common import types as T


StateKey = T.Tuple[str, ...]
StateValue = T.Any


class State(T.MutableMapping[StateKey, StateValue], metaclass=ABCMeta):
    """
    State engine abstract base class, exposing MutableMapping interface
    (i.e., implementations are used like a Python dictionary).

    NOTE Any state engine implementation will be used in a distributed
    fashion, where individual clients will not have access to each
    other. The implementation is expected to enforce atomicity; it is
    not expected to maintain availability (e.g., it can block). These
    constraints are NOT imposed by this type.

    Implementations required:
    * fetch    :: StateKey -> StateValue
    * set      :: StateKey x StateValue -> None
    * delete   :: StateKey -> None
    * __iter__ :: () -> Iterator[StateKey]
    * __len__  :: () -> int
    """
    @abstractmethod
    def fetch(self, key:StateKey) -> StateValue:
        """ Fetch state value by key """

    @abstractmethod
    def set(self, key:StateKey, value:StateValue) -> None:
        """ Set state key to value """

    @abstractmethod
    def delete(self, key:StateKey) -> None:
        """ Delete state by key """

    def __getitem__(self, key:StateKey) -> StateValue:
        return self.fetch(key)

    def __setitem__(self, key:StateKey, value:StateValue) -> None:
        return self.set(key, value)

    def __delitem__(self, key:StateKey) -> None:
        return self.delete(key)
