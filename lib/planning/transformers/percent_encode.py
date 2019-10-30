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
