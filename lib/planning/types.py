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

# We need postponed annotation evaluation for our recursive definitions
# https://docs.python.org/3/whatsnew/3.7.html#pep-563-postponed-evaluation-of-annotations
from __future__ import annotations

from abc import ABCMeta, abstractmethod
from copy import copy
from functools import singledispatch

from common import types as T
from common.templating import BaseTemplating
from common.models.graph import Vertex, BaseCost, CostBearing, Edge
from common.models.task import Task
from common.models.filesystems.types import Data, DataGenerator, BaseFilesystem


IOGenerator = T.Iterator[T.Tuple[Data, Data]]

TaskGenerator = T.Iterator[Task]


class PolynomialComplexity(BaseCost):
    """ Edge cost representing polynomial time complexity """
    # NOTE The k in O(n^k)
    def __add__(self, rhs:PolynomialComplexity) -> PolynomialComplexity:
        return max(self, rhs)

# Some useful constants
O1  = PolynomialComplexity(0)  # Constant time
On  = PolynomialComplexity(1)  # Linear time
On2 = PolynomialComplexity(2)  # Quadratic time


class FilesystemVertex(Vertex):
    """ Filesystem vertex """
    def __init__(self, filesystem:BaseFilesystem) -> None:
        self.payload = filesystem

    @property
    def filesystem(self) -> BaseFilesystem:
        # Convenience alias
        return self.payload


class BaseRouteTransformation(CostBearing, metaclass=ABCMeta):
    """
    Route transformation abstract base class

    Implementations required:
    __call__ :: <arbitrary> -> <arbitrary>
    __add__  :: BaseRouteTransformation -> BaseRouteTransformation
    """
    @abstractmethod
    def __call__(self, *args:T.Any, **kwargs:T.Any) -> T.Any:
        """ Interface for how the transformation is invoked """

    @abstractmethod
    def __add__(self, rhs:BaseRouteTransformation) -> BaseRouteTransformation:
        """ Interface for how transformations should be combined """


IOTransformer = T.Callable[[IOGenerator], IOGenerator]

class RouteIOTransformation(T.Carrier[IOTransformer], BaseRouteTransformation):
    """ Transform the I/O stream """
    def __init__(self, transformer:IOTransformer, cost:PolynomialComplexity = On) -> None:
        self.payload = transformer
        self.cost = cost

    def __call__(self, io:IOGenerator) -> IOGenerator:
        transformer = self.payload
        return transformer(io)

    def __add__(self, rhs:RouteIOTransformation) -> RouteIOTransformation:
        # Composition is this way around so the summation over the list
        # of transformers (i.e., in the TransferRoute edge) is done in
        # the same order as transformers are added
        composition = lambda io: rhs.payload(self.payload(io))
        return RouteIOTransformation(composition, self.cost + rhs.cost)


class RouteScriptTransformation(T.Carrier[BaseTemplating], BaseRouteTransformation):
    """
    Transform the transfer script

    This wraps the 'script' variable, when called, with the 'wrapper'
    template. Note that it is probably a good idea to give your wrapper
    templates different variable demarcation strings, else the variables
    in the root script will get nullified. For example:

    Script:

        #!/usr/bin/env bash
        transfer "{{ source }}" "{{ target }}"

    Wrapper:

        #!/usr/bin/env bash
        echo "Starting transfer of {{ source }}"
        [[ script ]]
        echo "Completed transfer to {{ target }}"

    """
    def __init__(self, templating:BaseTemplating, cost:PolynomialComplexity = O1) -> None:
        # TODO Subclass this, rather than relying on runtime checks
        assert "wrapper" in templating.templates
        self.payload = templating
        self.cost = cost

    def __call__(self, script:str) -> str:
        templating = self.payload
        return templating.render("wrapper", script=script)

    def __add__(self, rhs:RouteScriptTransformation) -> RouteScriptTransformation:
        # Composition is this way around so the summation over the list
        # of transformers (i.e., in the TransferRoute edge) is done in
        # the same order as transformers are added
        script = self.payload.get_template("wrapper")
        wrapped = rhs(script)

        templating = copy(rhs.payload)
        templating.add_template("wrapper", wrapped)

        return RouteScriptTransformation(templating, self.cost + rhs.cost)


_noop_io_transformer = RouteIOTransformation(lambda io: io)

class _NoopScriptTransformer(RouteScriptTransformation):
    """
    This is an templating engine-agnostic RouteScriptTransformation that
    simply returns whatever is passed through it
    """
    def __init__(self) -> None:
        pass

    def __call__(self, script:str) -> str:
        return script

    def __add__(self, rhs:RouteScriptTransformation) -> RouteScriptTransformation:
        return rhs

    def __radd__(self, lhs:RouteScriptTransformation) -> RouteScriptTransformation:
        return lhs

# These zeros are needed for the algebra over the transformers
_zeros = {
    RouteIOTransformation:     _noop_io_transformer,
    RouteScriptTransformation: _NoopScriptTransformer()
}


class TransferRoute(Edge, T.Carrier[T.List[BaseRouteTransformation]]):
    """ Data transfer route """
    _templating:BaseTemplating

    def __init__(self, source:FilesystemVertex, target:FilesystemVertex, *, templating:BaseTemplating, cost:PolynomialComplexity = On) -> None:
        """
        @param  source      Source filesystem vertex
        @param  target      Target filesystem vertex
        @param  templating  Templating engine that provides the script
        @param  cost        Route cost

        NOTE The templating engine that is injected into as instance of
        this class MUST define a template named "script", in which you
        may use the following variables, of type Date, which have
        .filesystem and .address attributes:

        * source  Source data
        * target  Target data
        """
        # TODO Subclass this, rather than relying on runtime checks
        assert "script" in templating.templates
        self._templating = templating

        self.payload = []
        self.cost = cost

        super().__init__(source, target, directed=True)

        # TODO From Python 3.8 there will be a singledispatchmethod
        # decorator; move to this when it's available, rather than using
        # the following workaround
        self.plan = singledispatch(self._plan_by_data_generator)
        self.plan.register(DataGenerator, self._plan_by_data_generator)
        self.plan.register(str, self._plan_by_query)

    @property
    def source(self) -> BaseFilesystem:
        # Convenience alias
        return self.a.filesystem

    @property
    def target(self) -> BaseFilesystem:
        # Convenience alias
        return self.b.filesystem

    def __iadd__(self, transform:BaseRouteTransformation) -> TransferRoute:
        """ Add a transformation to the route """
        self._payload.append(transform)
        self.cost += transform.cost
        return self

    def get_transform(self, transform_type:T.Type[BaseRouteTransformation]) -> BaseRouteTransformation:
        """ Filter the transforms by type and compose """
        transformers = (t for t in self.payload if isinstance(t, transform_type))
        return sum(transformers, _zeros[transform_type])

    def _plan_by_query(self, query:str) -> TaskGenerator:
        """
        Identify data from the source filesystem vertex, based on the
        given query, and pair this with the rendered transformation
        script and target filesystem location

        @param   query  Search criteria
        @return  Iterator of transfer plan steps
        """
        return self._plan_by_data_generator(self.source.identify(query))

    def _plan_by_data_generator(self, data:DataGenerator) -> TaskGenerator:
        """
        Pair the incoming data stream with the rendered transformation
        script and target filesystem location

        @param   data  Input data generator
        @return  Iterator of transfer plan steps
        """
        # Wrap the transfer script with any necessary transformations
        script = self._templating.get_template("script")
        wrapper = self.get_transform(RouteScriptTransformation)
        self._templating.add_template("transfer", wrapper(script))

        # NOTE Unless it's modified by the transfer script (i.e., by
        # applying filters to the "target" variable), or by an I/O route
        # transformation, the target location is assumed to be identical
        # to the source location
        io_generator = ((source, Data(self.target, source.address)) for source in data)
        io_transformer = self.get_transform(RouteIOTransformation)

        for source, target in io_transformer(io_generator):
            rendered = self._templating.render("transfer", source=source, target=target)
            yield Task(rendered, source, target)
