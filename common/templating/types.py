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

from .. import types as T


class TemplatingError(Exception):
    """ Raised on generic templating errors """


# Filter type MUST have at least one string argument,
# which (AFAIK) can't be annotated
Filter = T.Callable[..., str]


class BaseTemplating(metaclass=ABCMeta):
    """
    Templating engine base class

    Implementations required:
    * templates     :: () -> Iterator[str]
    * add_template  :: str x str -> None
    * get_template  :: str -> str
    * get_variables :: str -> Iterator[str]
    * filters       :: () -> Iterator[str]
    * add_filter    :: str x Function -> None
    * render        :: str x kwargs -> str
    """
    @property
    @abstractmethod
    def templates(self) -> T.Iterator[str]:
        """ Generator of available templates """

    @abstractmethod
    def add_template(self, name:str, template:str) -> None:
        """ Add a string template to the templating engine """

    @abstractmethod
    def get_template(self, name:str) -> str:
        """ Return the original template string """

    @abstractmethod
    def get_variables(self, name:str) -> T.Iterator[str]:
        """ Generator variables used by a template """

    @property
    def variables(self) -> T.Iterator[str]:
        """ Generator of all variables used by all templates """
        return iter({variable for template in self.templates
                              for variable in self.get_variables(template)})

    @property
    @abstractmethod
    def filters(self) -> T.Iterator[str]:
        """ Generator of available filters """

    @abstractmethod
    def add_filter(self, name:str, fn:Filter) -> None:
        """ Add a filter to the templating engine """

    @abstractmethod
    def render(self, name:str, **variables:T.Any) -> str:
        """ Render a specific template with the given variables """


def templating_factory(cls:T.Type[BaseTemplating], *, filters:T.Optional[T.Dict[str, Filter]] = None, templates:T.Optional[T.Dict[str, str]] = None, **cls_kwargs:T.Any) -> BaseTemplating:
    """
    Create Templating engine with given filters and templates

    @param   cls         Templating engine to instantiate
    @param   filters     Dictionary of filters (optional)
    @param   templates   Dictionary of templates (optional)
    @param   cls_kwargs  Templating class initialisation parameters
    @return  Templating engine
    """
    # Replace any sentinel values
    filters    = filters    or {}
    templates  = templates  or {}
    cls_kwargs = cls_kwargs or {}

    templating = cls(**cls_kwargs)

    for name, fn in filters.items():
        templating.add_filter(name, fn)

    for name, template in templates.items():
        templating.add_template(name, template)

    return templating
