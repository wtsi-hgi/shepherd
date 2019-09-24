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

from os import path

from common import types as T
from ..transfer import RouteIOTransformation, DataLocation, IOGenerator


# FIXME? We assume absolute paths for the targets, but this isn't
# necessarily true and would need to change when using something more
# generic/URI based, rather than as a wrapper to pathlib.Path
_ROOT = DataLocation("/")


def _strip_common_prefix(io:IOGenerator) -> IOGenerator:
    """ Strip the common prefix from all target locations """
    # TODO This will need to change when DataLocation becomes more
    # generic/URI based, rather than just a wrapper to pathlib.Path
    _buffer:T.List[T.Tuple[DataLocation, DataLocation]] = []
    _prefix:T.Optional[DataLocation] = None

    for source, target in io:
        # We calculate the common prefix one location at a time, because
        # os.path.commonpath otherwise eats a lot of memory
        _buffer.append((source, target))
        _prefix = DataLocation(path.commonpath((_prefix or target, target)))

    for source, target in _buffer:
        yield source, _ROOT / target.relative_to(_prefix)

strip_common_prefix = RouteIOTransformation(_strip_common_prefix)


def last_n_components(n:int) -> RouteIOTransformation:
    """
    Route IO transformation factory that takes, at most, the last n
    components from the target location

    @param   n  Number of components
    @return  IO transformer
    """
    # TODO This will need to change when DataLocation becomes more
    # generic/URI based, rather than just a wrapper to pathlib.Path
    assert n > 0

    def _last_n(io:IOGenerator) -> IOGenerator:
        for source, target in io:
            yield source, _ROOT / DataLocation(*target.parts[-n:])

    return RouteIOTransformation(_last_n)
