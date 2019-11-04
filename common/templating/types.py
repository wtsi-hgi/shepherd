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
from functools import reduce

from .. import types as T


class TemplatingError(Exception):
    """ Raised on generic templating errors """


# Filter type MUST have at least one string argument
Filter = T.Callable[..., str]


class BaseTemplating(metaclass=ABCMeta):
    """
    Templating engine base class

    Implementations required:
    * templates     :: () -> List[str]
    * filters       :: () -> List[str]
    * add_template  :: str x str -> None
    * get_template  :: str -> str
    * get_variables :: str -> Set[str]
    * add_filter    :: str x Function -> None
    * render        :: str x kwargs -> str
    """
    # TODO Use T.Iterator instead of T.List?
    @property
    @abstractmethod
    def templates(self) -> T.List[str]:
        """ List of available templates """

    @property
    @abstractmethod
    def filters(self) -> T.List[str]:
        """ List of available filters """

    @property
    def variables(self) -> T.Set[str]:
        """ Return all variables used by all templates """
        return reduce(
            set.union,
            (self.get_variables(template) for template in self.templates),
            set())

    @abstractmethod
    def add_template(self, name:str, template:str) -> None:
        """ Add a string template to the templating engine """

    @abstractmethod
    def get_template(self, name:str) -> str:
        """ Return the original template string """

    @abstractmethod
    def get_variables(self, name:str) -> T.Set[str]:
        """ Get variables used by a template """

    @abstractmethod
    def add_filter(self, name:str, fn:Filter) -> None:
        """ Add a filter to the templating engine """

    @abstractmethod
    def render(self, name:str, **tags:T.Any) -> str:
        """ Render a specific template with the given tags """


def templating_factory(cls:T.Type[BaseTemplating], *, filters:T.Optional[T.Dict[str, Filter]] = None, templates:T.Optional[T.Dict[str, str]] = None, **cls_kwargs:T.Any) -> BaseTemplating:
    """
    Create Templating engine with given filters and templates

    @param   cls         Templating engine to instantiate
    @param   filters     Dictionary of filters (optional)
    @param   templates   Dictionary of templates (optional)
    @param   cls_kwargs  Templating class initialisation parameters
    @return  Templating engine
    """
    templating = cls(**(cls_kwargs or {}))

    if filters is not None:
        for name, fn in filters.items():
            templating.add_filter(name, fn)

    if templates is not None:
        for name, template in templates.items():
            templating.add_template(name, template)

    return templating
