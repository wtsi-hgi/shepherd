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
import urllib.parse

from common import types as T
from common.models.filesystems.types import Data
from ..types import RouteIOTransformation, IOGenerator


_ROOT = T.Path("/")

def character_translator(to_replace:str, replace_with:str, name_only:bool) -> RouteIOTransformation:
    """
    Replaces substrings 'to_replace' with 'replace_with' in the entire path if
    'name_only' is set to False, or only the file name if it's True.

    @param  to_replace
    @param  replace_with
    @param  name_only If True, only translates characters in the file name. If
        false, translates characters in the entire path.
    @return IO transformer
    """
    def _tr(io:IOGenerator) -> IOGenerator:
        if(name_only):
            for source, target in io:
                new_target_name = target.address.name.replace(to_replace, replace_with)

                # TODO: What if the new file name is >255 chars long?

                new_target = Data(
                    filesystem = target.filesystem,
                    address    = target.address.parents[0] / new_target_name)

                yield source, new_target
        else:
            for source, target in io:
                new_address = T.Path( *[part.replace(to_replace, replace_with)
                    for part in target.address.parts] )

                new_target = Data(
                    filesystem  = target.filesystem,
                    address     = _ROOT / new_address)

                yield source, new_target

    return RouteIOTransformation(_tr)
