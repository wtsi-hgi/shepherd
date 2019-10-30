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

from os.path import commonpath

from common import types as T
from common.models.filesystems.types import Data
from ..types import RouteIOTransformation, IOGenerator


_ROOT = T.Path("/")


def _strip_common_prefix(io:IOGenerator) -> IOGenerator:
    """ Strip the common prefix from all target locations """
    _buffer:T.List[T.Tuple[Data, Data]] = []
    _prefix:T.Optional[Data] = None

    for source, target in io:
        # We calculate the common prefix one location at a time, because
        # os.path.commonpath otherwise eats a lot of memory
        _buffer.append((source, target))
        _prefix = T.Path(commonpath((_prefix or target.address, target.address)))

    for source, target in _buffer:
        new_target = Data(
            filesystem = target.filesystem,
            address    = _ROOT / target.address.relative_to(_prefix))

        yield source, new_target

strip_common_prefix = RouteIOTransformation(_strip_common_prefix)


def last_n_components(n:int) -> RouteIOTransformation:
    """
    Route IO transformation factory that takes, at most, the last n
    components from the target location

    @param   n  Number of components
    @return  IO transformer
    """
    assert n > 0

    def _last_n(io:IOGenerator) -> IOGenerator:
        for source, target in io:
            new_target = Data(
                filesystem = target.filesystem,
                address    = _ROOT / T.Path(*target.address.parts[-n:]))

            yield source, new_target

    return RouteIOTransformation(_last_n)
