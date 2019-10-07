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
from models.filesystem import Data
from ..transfer import RouteIOTransformation, IOGenerator


def prefix(prefix:T.Path) -> RouteIOTransformation:
    """
    Route IO transformation factory that prefixes the target path with
    the given path

    @param   prefix  Data location to prefix
    @return  IO transformer
    """
    assert prefix.is_absolute()

    def _prefixer(io:IOGenerator) -> IOGenerator:
        for source, target in io:
            new_target = Data(
                filesystem = target.filesystem,
                address    = prefix / target.address.relative_to(target.address.root))

            yield source, new_target

    return RouteIOTransformation(_prefixer)
