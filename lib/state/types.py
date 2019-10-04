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
from common.exceptions import NOT_IMPLEMENTED


# TODO There's an abstraction in here that I'm missing. I'm skirting
# around the edges of it, but it remains elusive and it bothers me. This
# will do for now, but more thought needs to be put into this to make it
# more general. For example, the primitives defined here are not
# dissimilar to those defined in other modules; but what troubles me
# deeper is to do with the specificity of how the persistence interface
# is defined...


class DataNotReady(BaseException):
    """ Raised when data is not yet available """

class NoCommonChecksum(BaseException):
    """ Raised when two data objects do not have a common checksum """


class Data(metaclass=ABCMeta):
    """
    Abstract base class for data objects

    Implementations required:
    * exists :: () -> Bool
    * _size :: () -> int
    * _checksum :: () -> Dict[str, str]
    """
    # FIXME This is effectively the same as FilesystemVertex in
    # lib.planning.transfer, but with the emphasis on data persistence,
    # rather than modelling a graph. Abstract this away and avoid
    # duplicating effort!
    _filesystem:str
    _address:T.Path

    _datasize:T.Optional[int]
    _checksums:T.Optional[T.Dict[str, str]]

    def __init__(self, filesystem:str, address:T.Path) -> None:
        self._filesystem = filesystem
        self._address = address

        self._datasize = self._checksums = None

    @property
    def filesystem(self) -> str:
        return self._filesystem

    @property
    def address(self) -> T.Path:
        return self._address

    @property
    @abstractmethod
    def exists(self) -> bool:
        """ Check data exists """

    @property
    @abstractmethod
    def _size(self) -> int:
        """ Get the size of the data in bytes """

    @property
    def size(self) -> int:
        """ Return the size of a data object """
        if not self.exists:
            raise DataNotReady(f"Data {self.address} is not ready on {self.filesystem}")

        if self._datasize is None:
            self._datasize = self._size

        return self._datasize

    @property
    @abstractmethod
    def _checksum(self) -> T.Dict[str, str]:
        """ Checksum the data """

    @property
    def checksum(self) -> T.Dict[str, str]:
        """ Return the checksums of a data object """
        if not self.exists:
            raise DataNotReady(f"Data {self.address} is not ready on {self.filesystem}")

        if not self._checksums is None:
            self._checksums = self._checksums

        return self._checksums

    # TODO Metadata stuff...

    def __eq__(self, rhs:Data) -> bool:
        algorithms = self.checksum.keys() & rhs.checksum.keys()
        if not algorithms:
            raise NoCommonChecksum(f"No common checksumming algorithm exists between {self.address} on {self.filesystem} and {rhs.address} on {rhs.filesystem}")

        return self.size == rhs.size \
           and all(self.checksum[algorithm] == rhs.checksum[algorithm] for algorithm in algorithms)


class Task(metadata=ABCMeta):
    """
    Abstract base class for tasks

    Implementations required:
    TODO: Does this need to be an ABC?
    """
    # FIXME This is effectively the same as TransferRoute in
    # lib.planning.transfer, but with the emphasis on data persistence,
    # rather than modelling a graph. Abstract this away and avoid
    # duplicating effort!
    _script:str
    _source:Data
    _target:Data
    _dependency:T.Optional[Task]

    _start:T.DateTime
    _finish:T.Optional[T.DateTime]
    _exit_code:T.Optional[int]

    def __init__(self, script:str, source:Data, target:Data, *, dependency:T.Optional[Task] = None) -> None:
        self._script = script
        self._source = source
        self._target = target
        self._dependency = dependency

    @property
    def start(self) -> T.DateTime:
        return self._start

    @property
    def finish(self) -> T.Optional[T.DateTime]:
        return self._finish

    @property
    def exit_code(self) -> T.Optional[int]:
        return self._exit_code

    def __call__(self) -> bool:
        # run script
        # set exit code
        # if successful, check equality of source and target
        # set metadata
        # return success
        # ALSO Update the state appropriately
        raise NOT_IMPLEMENTED


class Job(metadata=ABCMeta):
    """
    Abstract base class for job

    Implementations required:
    TODO
    """
    _state:T.Any
    _job_id:T.Any
    _max_attempts:int
    _max_concurrency:int
    _tasks:T.List[Task]

    @abstractmethod
    def __init__(self, state:T.Any, *, job:T.Optional[T.Any] = None, force_restart:bool = False) -> None:
        """ Constructor """

    # TODO Properties
    # * id (ro)
    # * max_attempts (rw)
    # * max_concurrency (rw)

    def __iadd__(self, task:Task) -> Job:
        self._tasks.append(task)
        return self
