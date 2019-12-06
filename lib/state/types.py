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
        # NOTE Completion does not imply success!
        try:
            return not self.phase(JobPhase.Transfer)

        except PhaseNotStarted:
            return False


class BaseAttempt(metaclass=ABCMeta):
    """
    Abstract base class for task attempts
    """
    # TODO Fill in the blanks


@dataclass
class DependentTask:
    """ Model of sequence of task dependencies """
    task:Task
    dependency:T.Optional[DependentTask] = None


class BaseJob(T.Iterator[BaseAttempt], metaclass=ABCMeta):
    """
    Abstract base class for jobs

    Implementations required:
    * __init__     :: Any x String x Optional[Identifier] x bool -> None
    * __iadd__     :: DependentTask -> BaseJob
    * __next__     :: () -> BaseAttempt
    * max_attempts :: Getter () -> int / Setter int -> None
    * status       :: Getter () -> BaseJobStatus
    """
    _client_id:str
    _job_id:T.Identifier

    @abstractmethod
    def __init__(self, state:T.Any, *, client_id:str, max_attempts:int = 3, job_id:T.Optional[T.Identifier] = None, force_restart:bool = False) -> None:
        """
        Constructor

        @param  state          Object that describes the persisted state
        @param  client_id      Client identifier
        @param  job_id         Job ID for a running job
        @param  max_attempts   Maximum attempts
        @param  force_restart  Whether to forcibly resume a job
        """

    @abstractmethod
    def __iadd__(self, task:DependentTask) -> BaseJob:
        """ Add a sequence of dependent tasks to the job """

    def __iter__(self) -> BaseJob:
        return self

    @abstractmethod
    def __next__(self) -> BaseAttempt:
        """ Fetch the next pending task to attempt """

    @property
    def job_id(self) -> T.Identifier:
        return self._job_id

    @property
    @abstractmethod
    def max_attempts(self) -> int:
        """ Get the maximum number of attempts for a task """

    @max_attempts.setter
    @abstractmethod
    def max_attempts(self, value:int) -> None:
        """ Set the maximum number of attempts for a task """

    @property
    @abstractmethod
    def status(self) -> BaseJobStatus:
        """ Get the current job status """
