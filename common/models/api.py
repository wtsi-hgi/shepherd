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

from dataclasses import dataclass

from .. import types as T


@dataclass
class _Argument:
    """ Base model for constructor arguments """
    name:str                          # Argument name
    type:T.Type                       # Argument type
    public:bool = True                # Publicly exposed/internal use
    default:T.Optional[T.Any] = None  # Default value
    help:T.Optional[str] = None       # Help text

@dataclass
class RequiredArgument(_Argument):
    """ Required argument model """

@dataclass
class OptionalArgument(_Argument):
    """ Optional argument model """


@dataclass
class API:
    """ Model for API calls """
    # TODO It would be neat if the arguments could be discerned
    # automatically by introspection
    callable:T.Callable
    arguments:T.Optional[T.List[_Argument]] = None
    help:T.Optional[str] = None

    def __call__(self, **kwargs):
        return self.callable(**kwargs)
