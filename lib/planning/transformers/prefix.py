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

from ..transfer import RouteIOTransformation, DataLocation, IOGenerator

def prefix(prefix:DataLocation) -> RouteIOTransformation:
    """
    Route IO transformation factory that prefixes the target path with
    the given path

    @param   prefix  Data location to prefix
    @return  IO transformer
    """
    # TODO This will need to change when DataLocation becomes more
    # generic/URI based, rather than just a wrapper to pathlib.Path
    assert prefix.is_absolute()

    def _prefixer(io:IOGenerator) -> IOGenerator:
        for source, target in io:
            yield source, prefix / target.relative_to(target.root)

    return RouteIOTransformation(_prefixer)
