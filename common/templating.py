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


# Filter type MUST have at least one string argument
Filter = T.Callable[..., str]


class Templating(metaclass=ABCMeta):
    """
    Templating engine base class

    Implementations required:
    * templates    :: () -> List[str]
    * filters      :: () -> List[str]
    * add_template :: str x str -> None
    * add_filter   :: str x Function -> None
    * render       :: str x kwargs -> str
    """
    @property
    @abstractmethod
    def templates(self) -> T.List[str]:
        """ List of available templates """

    @property
    @abstractmethod
    def filters(self) -> T.List[str]:
        """ List of available filters """

    @abstractmethod
    def add_template(self, name:str, template:str) -> None:
        """ Add a string template to the templating engine """

    @abstractmethod
    def get_template(self, name:str) -> str:
        """ Return the original template string """

    @abstractmethod
    def add_filter(self, name:str, fn:Filter) -> None:
        """ Add a filter to the templating engine """

    @abstractmethod
    def render(self, name:str, **tags:T.Any) -> str:
        """ Render a specific template with the given tags """


class Jinja2Templating(Templating):
    """ Jinja2-based templating engine """
    _env:Environment
    _templates:T.Dict[str, T.Tuple[str, Template]]

    def __init__(self) -> None:
        self._env = Environment()
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


def templating_factory(cls:T.Type[Templating], *, filters:T.Optional[T.Dict[str, Filter]] = None, templates:T.Optional[T.Dict[str, str]] = None) -> Templating:
    """
    Create Templating engine with given filters and templates

    @param   cls        Templating engine to instantiate
    @param   filters    Dictionary of filters (optional)
    @param   templates  Dictionary of templates (optional)
    @return  Templating engine
    """
    templating = cls()

    if filters is not None:
        for name, fn in filters.items():
            templating.add_filter(name, fn)

    if templates is not None:
        for name, template in templates.items():
            templating.add_template(name, template)

    return templating
