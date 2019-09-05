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

from common import types as T
from .abc import Vertex, Edge, Graph


def shortest_path(start:Vertex, finish:Vertex, via:T.Optional[T.List[Vertex]] = None) -> T.Optional[Graph]:
    """
    Find the shortest path between two vertices, through an optional
    list of waypoints, if it exists

    @param   start   Starting vertex
    @param   finish  Finishing vertex
    @param   via     Ordered list of waypoints
    @return  Shortest path (or None if no route can be found)
    """
