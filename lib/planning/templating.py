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

import os

from common import types as T
from common.templating import Jinja2Templating


_filters:T.Dict[str, T.Callable[..., str]] = {
    "dirname":  os.path.dirname,
    "basename": os.path.basename
    # TODO Any other useful filters...
}


templating = Jinja2Templating()

for name, fn in _filters.items():
    templating.add_filter(name, fn)
