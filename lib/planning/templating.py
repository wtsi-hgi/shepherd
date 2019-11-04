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
<<<<<<< HEAD
from common.templating import Jinja2Templating, Filter, templating_factory


_sh_escape = re.compile(r"([\"$])")

_filters:T.Dict[str, Filter] = {
    "dirname":   os.path.dirname,
    "basename":  os.path.basename,
    "sh_escape": lambda x: _sh_escape.sub(r"\\\1", str(x)),
    "to_lowercase": lambda x: x.lower()
    # TODO Any other useful filters...
}

_jinja2 = partial(templating_factory, Jinja2Templating, filters=_filters)

transfer_script = lambda script: _jinja2(templates={"script": script})
wrapper_script = lambda wrapper: _jinja2(templates={"wrapper": wrapper}, variable_start_string="[[", variable_end_string="]]")
=======
from common.templating import jinja2
>>>>>>> develop


# Convenience factories for transfer and wrapper scripts
transfer_script = lambda script: jinja2.templating(templates={"script": script})
wrapper_script = lambda wrapper: jinja2.templating(templates={"wrapper": wrapper}, variable_start_string="[[", variable_end_string="]]")
