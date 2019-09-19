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

from common import types as T
from .graph import Vertex, Cost, Edge, Graph, \
                   VertexNotInGraph, NoRouteFound
from .templating import templating


class UnsupportedByFilesystem(BaseException):
    """ Raised when an unsupported action is attempted on a filesystem """


class PolynomialComplexity(Cost):
    """ Edge cost representing polynomial time complexity """
    # NOTE The k in O(n^k)
    def __add__(self, rhs:PolynomialComplexity) -> PolynomialComplexity:
        return max(self, rhs)

# Some useful constants
O1  = PolynomialComplexity(0)  # Constant time
On  = PolynomialComplexity(1)  # Linear time
On2 = PolynomialComplexity(2)  # Quadratic time


class FilesystemVertex(Vertex, metaclass=ABCMeta):
    """
    Filesystem vertex abstract base class

    Implementations required:
    * _accessible           :: Path -> bool
    * _identify_by_metadata :: Path x kwargs -> Iterable[Path]
    * _identify_by_stat     :: Path x (TODO) -> Iterable[Path]
    * _identify_by_fofn     :: Path -> Iterable[Path]
    * supported_checksums   :: () -> List[str]
    * _checksum             :: str x Path -> str
    * set_metadata          :: Path x kwargs -> None
    * delete_metadata       :: Path x args -> None
    """
    # TODO/FIXME We are using Path throughout. It would probably be more
    # appropriate/general to use something like URI

    @abstractmethod
    def _accessible(self, data:T.Path) -> bool:
        """
        Check that a file exists and is readable

        @param   data  File to check
        @return  Predicate
        """

    @abstractmethod
    def _identify_by_metadata(self, **metadata:str) -> T.Iterable[T.Path]:
        """
        Identify data by the given set of key-value metadata

        @param   metadata  Key-value pairs
        @return  Iterator of matching paths
        """

    @abstractmethod
    def _identify_by_stat(self, path:T.Path, *, name:str = "*") -> T.Iterable[T.Path]:
        """
        Identify data by a combination of various stat metrics, similar
        to the find(1) utility

        @param   path  Search root path
        @param   name  Filename, that can include glob patterns
        @return  Iterator of matching paths
        """
        # TODO Flesh out parameters and interface

    @abstractmethod
    def _identify_by_fofn(self, fofn:T.Path) -> T.Iterable[T.Path]:
        """
        Identify data by a file of filenames

        @param   fofn  File of filenames
        @return  Iterator of matching paths
        """

    def identify(self, query:str) -> T.Iterable[T.Path]:
        """
        Identify data based on the given query

        @param   query  Search criteria
        @return  Iterator of matching paths
        """
        # TODO Design and implement query language that ultimately calls
        # the appropriate "_identify_by_*" method(s) and combines their
        # results accordingly
        raise NotImplementedError("Oh dear...")

    @abstractmethod
    @property
    def supported_checksums(self) -> T.List[str]:
        """
        Checksums algorithms supported by the filesystem

        @return  List of supported checksum algorithms
        """

    @abstractmethod
    def _checksum(self, algorithm:str, data:T.Path) -> str:
        """ Checksum a file with the given algorithm """

    def checksum(self, algorithm:str, data:T.Path) -> str:
        """
        Checksum a file with the given algorithm

        @param   algorithm  Checksum algorithm
        @param   data       File to checksum
        @return  Checksum of file
        """
        if algorithm not in self.supported_checksums:
            raise UnsupportedByFilesystem(f"Filesystem does not support the {algorithm} checksum algorithm")

        return self._checksum(algorithm, data)

    @abstractmethod
    def set_metadata(self, data:T.Path, **metadata:str) -> None:
        """
        Set (insert/update) key-value metadata for a given file

        @param   data      File
        @param   metadata  Key-value pairs
        """

    @abstractmethod
    def delete_metadata(self, data:T.Path, *keys:str) -> None:
        """
        Remove metadata, by key, from a given file

        @param   data  File
        @param   keys  Keys to delete
        """
        # FIXME Is this needed?
