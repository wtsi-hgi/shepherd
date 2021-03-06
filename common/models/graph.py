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

from .. import types as T
from ..exceptions import NOT_IMPLEMENTED


# FIXME Some (all?) of these types need to be declared as co- or
# contravariant to correctly type check. I'm still unsure which and how,
# but it doesn't affect functionality, only static analysis.


class VertexNotInGraph(Exception):
    """ Raised when a vertex does not exist in the given graph """

class NoRouteFound(Exception):
    """ Raised when no route can be found matching the itinerary """


class Vertex(T.Carrier[T.Any]):
    """ Vertex class """


@total_ordering
class BaseCost(T.Carrier[T.Number], metaclass=ABCMeta):
    """
    Edge cost abstract base class

    Implementations required:
    * __add__ :: BaseCost -> BaseCost
    """
    def __init__(self, cost:T.Number) -> None:
        self.payload = cost

    def __eq__(self, rhs:BaseCost) -> bool:
        return self.payload == rhs.payload

    def __lt__(self, rhs:BaseCost) -> bool:
        return self.payload < rhs.payload

    @abstractmethod
    def __add__(self, rhs:BaseCost) -> BaseCost:
        """ Define how edge costs should be summed """

    @property
    def value(self) -> T.Number:
        """ Convenience alias """
        return self.payload


class CostBearing:
    """ Mixin for classes that incur a cost upon graph traversal """
    _cost:BaseCost

    @property
    def cost(self) -> BaseCost:
        return self._cost

    @cost.setter
    def cost(self, value:BaseCost) -> None:
        self._cost = value


class Edge(T.Carrier[T.Any], T.Container[Vertex], CostBearing):
    """ Edge class """
    _vertices:T.Tuple[Vertex, Vertex]
    _directed:bool

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


# Convenience type (rather than a full Graph): An ordered list of edges,
# where juxtaposed vertices correspond (i.e., a contiguous route)
Route = T.List[Edge]

class Graph(T.Container[Vertex]):
    """ Graph model """
    # NOTE Our graph is a container of edges; contrary to definition, we
    # do not consider unconnected vertices to be in the graph
    _edges:T.List[Edge]
    _atlas:T.Dict[Vertex, None]

    def __init__(self) -> None:
        self._edges = []
        self._atlas = {}

    def __contains__(self, needle:Vertex) -> bool:
        """ Does the graph contain a given vertex? """
        return needle in self._atlas

    def __iadd__(self, edge:Edge) -> Graph:
        """ Add an edge to a graph """
        self._edges.append(edge)
        self._atlas[edge.a] = None
        self._atlas[edge.b] = None
        return self

    def __add__(self, graph:Graph) -> Graph:
        """ Return the union of two graphs """
        # TODO This is probably not going to be needed...
        union = copy(graph)
        for edge in self._edges:
            union += edge

        return union

    def neighbours(self, vertex:Vertex) -> T.Iterator[Edge]:
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

    def _shortest_path(self, a:Vertex, b:Vertex) -> Route:
        """ Shortest path between two vertices """
        # TODO
        raise NOT_IMPLEMENTED

    def route(self, *waypoints:Vertex) -> Route:
        """
        Find the shortest path through an ordered list of waypoints, if
        it exists

        @param   waypoints  Waypoint vertices (at least 2)
        @return  Shortest path
        """
        if len(waypoints) < 2:
            raise NoRouteFound(f"Cannot route between {len(waypoints)} vertices")

        if any(vertex not in self for vertex in waypoints):
            raise VertexNotInGraph(f"Graph {self} does not contain all required waypoints")

        # Concatenation of shortest paths between pairwise waypoints
        return sum((
            self._shortest_path(*terminals)
            for terminals in zip(waypoints, waypoints[1:])), [])
