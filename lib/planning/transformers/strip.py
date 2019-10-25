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

def _percent_encode(io:IOGenerator) -> IOGenerator:
    """ Remove non-ASCII characters from target locations """
    valid_chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
    valid_chars += "0123456789()[]{}-_#%&+,.:;<>=@$"

    for source, target in io:
        new_address = T.Path( *[urllib.parse.quote(part, safe=valid_chars) for
            part in target.address.parts] )

        # TODO: What if the expanded file name is >255 chars long?
        new_target = Data(
            filesystem = target.filesystem,
            address    = _ROOT / new_address)

        yield source, new_target

percent_encode = RouteIOTransformation(_percent_encode)


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
