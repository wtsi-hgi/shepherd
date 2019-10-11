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

from common import types as T, time
from common.models.task import Task
from common.models.filesystems.types import BaseFilesystem


class DataNotReady(BaseException):
    """ Raised when data is not yet available """

class NoCommonChecksumAlgorithm(BaseException):
    """ Raised when filesystems do not share a common checksumming algorithm """


@dataclass
class JobStatus:
    """ Model for expressing job task status and properties """
    pending:int
    running:int
    failed:int
    succeeded:int

    start:T.DateTime
    finish:T.Optional[T.DateTime] = None

    def __bool__(self) -> bool:
        # Return truthy if there are still jobs pending
        # TODO ...or running??
        return self.pending > 0

    @property
    def runtime(self) -> T.TimeDelta:
        """ Job runtime """
        until = self.finish or time.now()
        return until - self.start


# TODO Adaptors for Data and Task that augment them with machinery to
# track and update persisted state (maybe an ABC mixin?)


Identifier = T.TypeVar("Identifier")

class BaseJob(T.Iterator[Task], metaclass=ABCMeta):
    """
    Abstract base class for jobs

    Implementations required:
    * __init__ :: Any x Optional[Identifier] x bool -> None
    * __iadd__ :: Task -> BaseJob
    * __next__ :: () -> Task
    * status   :: () -> JobStatus
    """
    _job_id:Identifier
    _max_attempts:int
    _max_concurrency:int
    _filesystems:T.Dict[str, BaseFilesystem]

    # TODO Properties that expose max_attempts and max_concurrency
    # FIXME The maximum concurrency is a property of the filesystems
    # involved in a task, rather than that of an entire job

    @abstractmethod
    def __init__(self, state:T.Any, *, job_id:T.Optional[Identifier] = None, force_restart:bool = False) -> None:
        """
        Constructor

        @param  state          Object that describes the persisted state
        @param  job_id         Job ID for a running job
        @param  force_restart  Whether to forcibly resume a job
        """

    @property
    def job_id(self) -> Identifier:
        return self._job_id

    @property
    def filesystem_mapping(self) -> T.Dict[str, BaseFilesystem]:
        # FIXME? This injection is a bit primitive...
        return self._filesystems

    @filesystem_mapping.setter
    def filesystem_mapping(self, mapping:T.Dict[str, BaseFilesystem]) -> None:
        """ Inject filesystem mappings """
        # FIXME? This injection is a bit primitive...
        self._filesystems = mapping

    @abstractmethod
    def __iadd__(self, task:Task) -> BaseJob:
        """ Add a task to the job """

    def __iter__(self) -> BaseJob:
        return self

    @abstractmethod
    def __next__(self) -> Task:
        """ Fetch the next pending task to execute """

    @property
    @abstractmethod
    def status(self) -> JobStatus:
        """ Get the current job status """

    # TODO
    # * Setting and/or checking of checksums, sizes and metadata
    # * Checksumming is not necessarily an operation provided by the
    #   filesystem and therefore needs to be done manually, which takes
    #   time; i.e., it's wrong to assume we can just get the checksum
    #   without potentially blocking for ages
    # * How to keep track of attempts?
