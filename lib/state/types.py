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
from enum import Enum, auto

from common import types as T, time
from common.models.task import Task
from common.models.filesystems.types import BaseFilesystem
from .exceptions import *


@dataclass(frozen=True)
class JobThroughput:
    """ Model of throughput rates between filesystems """
    source:BaseFilesystem
    target:BaseFilesystem
    transfer_rate:float  # bytes/second
    failure_rate:float   # Probability


class JobPhase(Enum):
    """ Enumeration of job phases """
    Preparation = auto()
    Transfer    = auto()


@dataclass(frozen=True)
class PhaseStatus:
    """ Model for expressing phase status """
    phase:JobPhase
    start:T.DateTime
    finish:T.Optional[T.DateTime] = None

    def __bool__(self) -> bool:
        # Truthy while there's no finish timestamp (i.e., in progress)
        return self.finish is None

    @property
    def runtime(self) -> T.TimeDelta:
        """ Phase runtime """
        until = self.finish or time.now()
        return until - self.start


@dataclass(frozen=True)
class _TaskOverviewMixin:
    """ Model for expressing job task overview """
    pending:int
    running:int
    failed:int
    succeeded:int

    def __bool__(self) -> bool:
        # Truthy when there are still jobs pending (i.e., in progress)
        return self.pending > 0

class BaseJobStatus(_TaskOverviewMixin, metaclass=ABCMeta):
    """
    Abstract base class for job and phase status

    Implementations required:
    * throughput :: BaseFilesystem x BaseFilesystem -> JobThroughput
    * phase      :: JobPhase -> PhaseStatus
    """
    @abstractmethod
    def throughput(self, source:BaseFilesystem, target:BaseFilesystem) -> JobThroughput:
        """
        Return the throughput rates between the given filesystems

        @param   source            Source filesystem
        @param   target            Target filesystem
        @raise   NoThroughputData  No throughtput data available
        @return  Job throughput
        """

    @abstractmethod
    def phase(self, phase:JobPhase) -> PhaseStatus:
        """
        Return the phase status for the specified phase

        @param   phase            Job phase
        @raise   PhaseNotStarted  Phase has yet to start
        @return  Phase status
        """

    @property
    def complete(self) -> bool:
        # Truthy when the transfer phase has ended (n.b., which is
        # dependent on the preparation phase ending)
        try:
            return not self.phase(JobPhase.Transfer)

        except PhaseNotStarted:
            return False







# TODO Adaptors for Data and Task that augment them with machinery to
# track and update persisted state (maybe an ABC mixin?)


class BaseJob(T.Iterator[Task], metaclass=ABCMeta):
    """
    Abstract base class for jobs

    Implementations required:
    * __init__ :: Any x Optional[Identifier] x bool -> None
    * __iadd__ :: Task -> BaseJob
    * __next__ :: () -> Task
    * status   :: () -> JobStatus
    """
    _job_id:T.Identifier
    _filesystems:T.Dict[str, BaseFilesystem]

    @abstractmethod
    def __init__(self, state:T.Any, *, job_id:T.Optional[T.Identifier] = None, force_restart:bool = False) -> None:
        """
        Constructor

        @param  state          Object that describes the persisted state
        @param  job_id         Job ID for a running job
        @param  force_restart  Whether to forcibly resume a job
        """

    @property
    def job_id(self) -> T.Identifier:
        return self._job_id

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
    # * Exposure of maximum attempts and concurrency
    #   (FIXME the maximum concurrency is a property of the filesystems
    #   involved in a task, rather than that of an entire job...)
    # * Setting and/or checking of checksums, sizes and metadata
    # * Checksumming is not necessarily an operation provided by the
    #   filesystem and therefore needs to be done manually, which takes
    #   time; i.e., it's wrong to assume we can just get the checksum
    #   without potentially blocking for ages
    # * How to keep track of attempts?

    # FIXME? This injection is a bit primitive...
    @property
    def filesystem_mapping(self) -> T.Dict[str, BaseFilesystem]:
        return self._filesystems

    @filesystem_mapping.setter
    def filesystem_mapping(self, mapping:T.Dict[str, BaseFilesystem]) -> None:
        """ Inject filesystem mappings """
        self._filesystems = mapping
