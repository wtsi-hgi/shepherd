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
from dataclasses import dataclass
from functools import partial

from jinja2 import Environment, Template, meta

from .. import types as T
from .types import BaseTemplating, Filter, TemplatingError, templating_factory


@dataclass
class _Template:
    """ Internal template model """
    source:str
    variables:T.Set[str]
    template:Template

    def __init__(self, source:str, environment:Environment) -> None:
        self.source = source

        parsed = environment.parse(source)
        self.variables = meta.find_undeclared_variables(parsed)

        self.template = environment.from_string(source)

    def render(self, **variables) -> str:
        v = set(variables)
        if v < self.variables or v.isdisjoint(self.variables):
            missing = ", ".join(self.variables - v)
            raise TemplatingError(f"Variables undefined for template: {missing}")

        return self.template.render(**variables)


class Jinja2Templating(BaseTemplating):
    """ Jinja2-based templating engine """
    _env:Environment
    _templates:T.Dict[str, _Template]

    def __init__(self, **kwargs:T.Any) -> None:
        self._env = Environment(**kwargs)
        self._templates = {}

    @property
    def templates(self) -> T.Iterator[str]:
        return iter(self._templates.keys())

    def add_template(self, name:str, template:str) -> None:
        self._templates[name] = _Template(template, self._env)

    def get_template(self, name:str) -> str:
        return self._templates[name].source

    def get_variables(self, name:str) -> T.Iterator[str]:
        return iter(self._templates[name].variables)

    @property
    def filters(self) -> T.Iterator[str]:
        return iter(self._env.filters.keys())

    def add_filter(self, name:str, fn:Filter) -> None:
        self._env.filters[name] = fn

    def render(self, name:str, **variables:T.Any) -> str:
        if name not in self._templates:
            raise TemplatingError(f"No such template {name}")

        return self._templates[name].render(**variables)


# Standard Jinja2-based templating factory and filters
_sh_escape = re.compile(r"([\"$])")

_filters:T.Dict[str, Filter] = {
    "dirname":   os.path.dirname,
    "basename":  os.path.basename,
    "sh_escape": lambda x: _sh_escape.sub(r"\\\1", str(x))
    # TODO Any other useful filters...
}

templating = partial(templating_factory, Jinja2Templating, filters=_filters)
