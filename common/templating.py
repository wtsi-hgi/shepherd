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

from abc import ABCMeta, abstractmethod

from jinja2 import Environment, Template

from . import types as T


class TemplatingError(BaseException):
    """ Raised on generic templating errors """


class Templating(metaclass=ABCMeta):
    """ Templating engine base class """
    @abstractmethod
    def add_template(self, name:str, template:str) -> None:
        """ Add a string template to the templating engine """

    @abstractmethod
    def add_filter(self, name:str, fn:T.Callable[..., str]) -> None:
        """ Add a filter to the templating engine """

    @abstractmethod
    def render(self, template:str, **tags:T.Any) -> str:
        """ Render a specific template with the given tags """


class Jinja2Templating(Templating):
    """ Jinja2-based templating engine """
    _env:Environment
    _templates:T.Dict[str, Template]

    def __init__(self) -> None:
        self._env = Environment()
        self._templates = {}

    def add_template(self, name:str, template:str) -> None:
        self._templates[name] = self._env.from_string(template)

    def add_filter(self, name:str, fn:T.Callable[..., str]) -> None:
        self._env.filters[name] = fn

    def render(self, template:str, **tags:T.Any) -> str:
        if template not in self._templates:
            raise TemplatingError(f"No such template {template}")

        return self._templates[template].render(**tags)
