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
from functools import partial

from common import types as T
from common.templating import Jinja2Templating, Filter, templating_factory


_filters:T.Dict[str, Filter] = {
    "dirname":  os.path.dirname,
    "basename": os.path.basename
    # TODO Any other useful filters...
}

_jinja2 = partial(templating_factory, Jinja2Templating, filters=_filters)

transfer_script = lambda script: _jinja2(templates={"script": script})
wrapper_script = lambda wrapper: _jinja2(templates={"wrapper": wrapper}, variable_start_string="[[", variable_end_string="]]")
