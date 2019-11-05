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
import re
from functools import partial

from jinja2 import Environment, Template

from .. import types as T
from .types import BaseTemplating, Filter, TemplatingError, templating_factory


class Jinja2Templating(BaseTemplating):
    """ Jinja2-based templating engine """
    _env:Environment
    _templates:T.Dict[str, T.Tuple[str, Template]]

    def __init__(self, **kwargs:T.Any) -> None:
        self._env = Environment(**kwargs)
        self._templates = {}

    @property
    def templates(self) -> T.List[str]:
        return list(self._templates.keys())

    @property
    def filters(self) -> T.List[str]:
        return list(self._env.filters.keys())

    def add_template(self, name:str, template:str) -> None:
        self._templates[name] = template, self._env.from_string(template)

    def get_template(self, name:str) -> str:
        template, _ = self._templates[name]
        return template

    def add_filter(self, name:str, fn:Filter) -> None:
        self._env.filters[name] = fn

    def render(self, name:str, **tags:T.Any) -> str:
        if name not in self._templates:
            raise TemplatingError(f"No such template {name}")

        _, template = self._templates[name]
        return template.render(**tags)


# Standard Jinja2-based templating factory and filters
_sh_escape = re.compile(r"([\"$])")

_filters:T.Dict[str, Filter] = {
    "dirname":   os.path.dirname,
    "basename":  os.path.basename,
    "sh_escape": lambda x: _sh_escape.sub(r"\\\1", str(x)),
    "to_lowercase": lambda x: x.lower()
    # TODO Any other useful filters...
}

templating = partial(templating_factory, Jinja2Templating, filters=_filters)
