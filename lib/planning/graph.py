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
from functools import total_ordering

from common import types as T


class VertexNotInGraph(BaseException):
    """ Raised when a vertex does not exist in the given graph """

class NoRouteFound(BaseException):
    """ Raised when no route can be found matching the itinerary """


class Vertex(T.Carrier[T.Any], metaclass=ABCMeta):
    """ Vertex abstract base class """


@total_ordering
class Cost(T.Carrier[T.Number], metaclass=ABCMeta):
    """ Edge cost abstract base class """
    def __init__(self, cost:T.Number) -> None:
        self.payload = cost

    def __eq__(self, rhs:Cost) -> bool:
        return self.payload == rhs.payload

    def __lt__(self, rhs:Cost) -> bool:
        return self.payload < rhs.payload

    @abstractmethod
    def __add__(self, rhs:Cost) -> Cost:
        """ Define how edge costs should be summed """

    @property
    def value(self) -> T.Number:
        """ Convenience alias """
        return self.payload


class Edge(T.Carrier[T.Any], T.Container[Vertex], metaclass=ABCMeta):
    """ Edge abstract base class """
    _vertices:T.Tuple[Vertex, Vertex]
    _directed:bool
    _cost:Cost

    def __init__(self, a:Vertex, b:Vertex, *, directed:bool = False) -> None:
        self._vertices = (a, b)
        self._directed = directed

    def __contains__(self, needle:Vertex) -> bool:
        return needle in self._vertices

    @property
    def a(self) -> Vertex:
        a, _ = self._vertices
        return a

    @property
    def b(self) -> Vertex:
        _, b = self._vertices
        return b

    @property
    def is_directed(self) -> bool:
        return self._directed

    @property
    def cost(self) -> Cost:
        return self._cost

    @cost.setter
    def cost(self, value:Cost) -> None:
        self._cost = value


class Graph(T.Container[Edge], metaclass=ABCMeta):
    """ Graph abstract base class """
    # NOTE Our graph is a container of edges; contrary to definition, we
    # do not consider unconnected vertices to be in the graph
    _edges:T.List[Edge]

    def __init__(self) -> None:
        self._edges = []

    def __contains__(self, needle:Vertex) -> bool:
        """ Does the graph contain a given vertex? """
        # NOTE We openly flaunt the interface of typing.Container, as
        # we're more interested in the vertices, rather than the edges
        # FIXME Make this O(1), rather than O(n)
        return any(needle in haystack for haystack in self._edges)

    def __iadd__(self, edge:Edge) -> Graph:
        """ Add an edge to a graph """
        self._edges.append(edge)
        return self

    def __add__(self, graph:Graph) -> Graph:
        """ Return the union of two graphs """
        union = copy(graph)
        for edge in self._edges:
            union += edge

        return union

    def neighbours(self, vertex:Vertex) -> T.Iterable[Edge]:
        """
        Return all immediate neighbours, by edge, of a vertex

        @param   vertex  Starting vertex
        @return  Generator of valid neighbouring edges
        """
        # NOTE This is more like a list of valid routes to/from a vertex
        if vertex not in self:
            raise VertexNotInGraph(f"Vertex {vertex} is not in Graph {self}")

        for needle in self._edges:
            if vertex is needle.a:
                yield needle

            elif not needle.is_directed and vertex is needle.b:
                # The edge is important to us, so we return it
                # unchanged, rather than reversing its vertices
                yield needle

    def route(self, a:Vertex, b:Vertex, *, via:T.Optional[T.List[Vertex]] = None) -> Graph:
        """
        Find the shortest path between two vertices, through an optional
        list of waypoints, if it exists

        @param   start   Starting vertex
        @param   finish  Finishing vertex
        @param   via     Ordered list of waypoints
        @return  Shortest path
        """
        for vertex in [a, b, *(via or [])]:
            if not vertex in self:
                raise VertexNotInGraph(f"Vertex {vertex} is not in Graph {self}")

        # TODO
        raise NotImplementedError("Oh dear...")
