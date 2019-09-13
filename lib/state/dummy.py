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

from common import types as T
from .types import State, StateKey, StateValue


_SOME_KEY:StateKey = ("foo", "bar")
_SOME_VAL:StateValue = 123


class InMemoryState(State):
    """
    Test State implementation
    NOT ATOMIC - DO NOT USE IN PRODUCTION
    """
    _data:T.Dict

    def __init__(self):
        self._data = {_SOME_KEY: _SOME_VAL}

    def __iter__(self) -> T.Iterator[StateKey]:
        return iter(self._data)

    def __len__(self) -> int:
        return len(self._data)

    def fetch(self, k):
        return self._data[k]

    def set(self, k, v):
        self._data[k] = v

    def delete(self, k):
        del self._data[k]


if __name__ == "__main__":
    state = InMemoryState()
    assert len(state) == 1
    assert state[_SOME_KEY] == _SOME_VAL
    del state[_SOME_KEY]
    assert len(state) == 0
    print("OK")
