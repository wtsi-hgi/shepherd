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
from dataclasses import dataclass

from ... import types as T
from ...exceptions import NOT_IMPLEMENTED


class UnsupportedByFilesystem(BaseException):
    """ Raised when an unsupported action is attempted on a filesystem """

class DataInaccessible(BaseException):
    """ Raised when data cannot be accessed for whatever reason """


# TODO? Do we need a metadata model, or will Dict[str, str] do?


@dataclass
class Data:
    """ Simple file object model """
    filesystem:Filesystem
    address:T.Path

DataGenerator = T.Iterable[Data]


class BaseFilesystem(metaclass=ABCMeta):
    """
    Filesystem abstract base class

    Implementations required:
    * _accessible           :: Path -> bool
    * _identify_by_metadata :: Path x kwargs -> Iterable[Data]
    * _identify_by_stat     :: TODO... -> Iterable[Data]
    * _identify_by_fofn     :: Path -> Iterable[Data]
    * supported_checksums   :: () -> List[str]
    * _checksum             :: str x Path -> str
    * _size                 :: Path -> int
    * set_metadata          :: Path x kwargs -> None
    * delete_metadata       :: Path x args -> None
    * delete_data           :: Path -> None
    """
    _name:str
    _max_concurrency:int

    @property
    def name(self) -> str:
        # This should be read-only, set in the constructor
        return self._name

    def __str__(self) -> str:
        return self._name

    @property
    def max_concurrency(self) -> int:
        return self._max_concurrency

    @max_concurrency.setter
    def max_concurrency(self, value:int) -> None:
        assert value > 0
        self._max_concurrency = value

    @abstractmethod
    def _accessible(self, address:T.Path) -> bool:
        """
        Check that a file exists and is readable

        @param   address  Location of file
        @return  Predicate
        """
        # FIXME It's better to ask forgiveness than to seek permission;
        # the model as described here is susceptible to security holes

    @abstractmethod
    def _identify_by_metadata(self, **metadata:str) -> DataGenerator:
        """
        Identify data by the given set of key-value metadata

        @param   metadata  Key-value pairs
        @return  Iterator of matching paths
        """

    @abstractmethod
    def _identify_by_stat(self, address:T.Path, *, name:str = "*") -> DataGenerator:
        """
        Identify data by a combination of various stat metrics, similar
        to the find(1) utility

        @param   address  Search root address
        @param   name     Filename, that can include glob patterns
        @return  Iterator of matching paths
        """
        # TODO Flesh out parameters and interface

    @abstractmethod
    def _identify_by_fofn(self, fofn:T.Path, *, delimiter:str = "\n", compressed:bool = False) -> DataGenerator:
        """
        Identify data by a file of filenames

        @param   fofn        File of filenames
        @param   delimiter   Record delimiter
        @param   compressed  Is FoFN gzip-compressed
        @return  Iterator of matching paths
        """

    def identify(self, query:str) -> DataGenerator:
        """
        Identify data based on the given query

        @param   query  Search criteria
        @return  Iterator of matching paths
        """
        # TODO Design and implement query language that ultimately calls
        # the appropriate "_identify_by_*" method(s) and combines their
        # results accordingly
        raise NOT_IMPLEMENTED

    @property
    @abstractmethod
    def supported_checksums(self) -> T.List[str]:
        """
        Checksums algorithms supported by the filesystem

        @return  List of supported checksum algorithms
        """

    @abstractmethod
    def _checksum(self, algorithm:str, address:T.Path) -> str:
        """ Checksum a file with the given algorithm """

    def checksum(self, algorithm:str, address:T.Path) -> str:
        """
        Checksum a file with the given algorithm

        @param   algorithm  Checksum algorithm
        @param   address    Location of file
        @return  Checksum of file
        """
        if algorithm not in self.supported_checksums:
            raise UnsupportedByFilesystem(f"Filesystem {self.name} does not support the {algorithm} checksum algorithm")

        if not self._accessible(address):
            raise DataInaccessible(f"Cannot access {address} on {self.name}")

        return self._checksum(algorithm, address)

    @abstractmethod
    def _size(self, address:T.Path) -> int:
        """ Return the size of a file in bytes """

    def size(self, address:T.Path) -> int:
        """
        Return the size of a file in bytes

        @param   address  Location of file
        @return  Size of file
        """
        if not self._accessible(address):
            raise DataInaccessible(f"Cannot access {address} on {self.name}")

        return self._size(address)

    @abstractmethod
    def set_metadata(self, address:T.Path, **metadata:str) -> None:
        """
        Set (insert/update) key-value metadata for a given file

        @param   address   Location of file
        @param   metadata  Key-value pairs
        """

    @abstractmethod
    def delete_metadata(self, address:T.Path, *keys:str) -> None:
        """
        Remove metadata, by key, from a given file

        @param   address  Location of file
        @param   keys     Keys to delete
        """
        # FIXME Is this needed?

    @abstractmethod
    def delete_data(self, address:T.Path) -> None:
        """
        Delete data from filesystem

        @param   address  Location of file
        """
