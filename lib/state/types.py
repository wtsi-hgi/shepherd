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
from common.logging import log, Level
from common.models.task import ExitCode, Task
from common.models.filesystems.types import BaseFilesystem
from .exceptions import *


# Verification failure handling
_MISMATCHED_SIZE     = ExitCode(-1)
_MISMATCHED_CHECKSUM = ExitCode(-2)

class _VerificationFailure(Exception):
    """ Raised on verification failure """


@dataclass
class _DurationMixin:
    """ Model for durations """
    start:T.DateTime
    finish:T.Optional[T.DateTime]

    @property
    def runtime(self) -> T.TimeDelta:
        """ Phase runtime """
        until = self.finish or time.now()
        return until - self.start

class _BaseDurationMixin(_DurationMixin, metaclass=ABCMeta):
    """
    Abstract base class for setting the start and finish timestamps

    Implementations required:
    * init :: () -> T.DateTime
    * stop :: () -> T.DateTime
    """
    @abstractmethod
    def init(self) -> T.DateTime:
        """ Persist and return the duration start timestamp """

    @abstractmethod
    def stop(self) -> T.DateTime:
        """ Persist and return the duration finish timestamp """

    def __enter__(self) -> None:
        self.init()

    def __exit__(self, *exception) -> bool:
        self.stop()
        return not exception  # FIXME? Is this right?


@dataclass(frozen=True)
class JobThroughput:
    """ Model of throughput rates between filesystems """
    transfer_rate:float  # bytes/second
    failure_rate:float   # Probability


class JobPhase(Enum):
    """ Enumeration of job phases """
    Preparation = auto()
    Transfer    = auto()


class BasePhaseStatus(_BaseDurationMixin, metaclass=ABCMeta):
    """ Abstract base class for phase status """
    def __bool__(self) -> bool:
        # Truthy while there's no finish timestamp (i.e., in progress)
        return self.finish is None


@dataclass(frozen=True)
class _TaskOverviewMixin:
    """ Model for expressing job task overview """
    pending:int
    running:int
    failed:int
    succeeded:int

class BaseJobStatus(_TaskOverviewMixin, metaclass=ABCMeta):
    """
    Abstract base class for job and phase status

    Implementations required:
    * throughput :: BaseFilesystem x BaseFilesystem -> JobThroughput
    * phase      :: JobPhase -> BasePhaseStatus
    """
    def __bool__(self) -> bool:
        # Truthy when the preparation phase is still in progress or
        # there are still jobs pending (i.e., in progress)
        return self.phase(JobPhase.Preparation) or self.pending > 0

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
    def phase(self, phase:JobPhase) -> BasePhaseStatus:
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


class DataOrigin(Enum):
    """ Enumeration of data origins """
    Source = auto()
    Target = auto()


@dataclass
class _AttemptMixin:
    """ Model of task attempts """
    task:Task

class BaseAttempt(_AttemptMixin, _BaseDurationMixin, metaclass=ABCMeta):
    """
    Abstract base class for task attempts

    Implementations required:
    * size      :: DataOrigin -> int
    * checksum  :: DataOrigin x str -> int
    * exit_code :: Getter () -> ExitCode / Setter ExitCode -> None
    """
    @abstractmethod
    def size(self, origin:DataOrigin) -> int:
        """ Persist and return the origin's data size """

    @abstractmethod
    def checksum(self, origin:DataOrigin, algorithm:str) -> str:
        """ Persist and return the origin's data checksum """

    @property
    @abstractmethod
    def exit_code(self) -> ExitCode:
        """
        Fetch the task's exit code for this attempt, if available

        @raise  DataNotReady  No exit code yet available
        """

    @exit_code.setter
    @abstractmethod
    def exit_code(self, value:ExitCode) -> None:
        """ Persist the task's exit code for this attempt """

    def __call__(self) -> bool:
        """ Attempt the task """
        # The context manager sets the attempt start and finish timestamps
        with self:
            task = self.task
            log(f"Attempting transfer of {task.source.address} from {task.source.filesystem} "
                f"to {task.target.filesystem} at {task.target.address}", Level.Info)

            # TODO Run these in a separate thread
            # TODO Different/multiple checksum algorithms
            source_size     = self.size(DataOrigin.Source)
            source_checksum = self.checksum(DataOrigin.Source, "md5")

            # Run task
            self.exit_code = success = self.task()

            if not success:
                log(f"Attempt failed with exit code {success.exit_code}", Level.Warning)

            else:
                try:
                    target_size = self.size(DataOrigin.Target)
                    if source_size != target_size:
                        log(f"Attempt failed: "
                            f"Source is {source_size} bytes, "
                            f"target is {target_size} bytes", Level.Warning)

                        self.exit_code = success = _MISMATCHED_SIZE
                        raise _VerificationFailure()

                    # TODO Different/multiple checksum algorithms
                    target_checksum = self.checksum(DataOrigin.Target, "md5")
                    if source_checksum != target_checksum:
                        log(f"Attempt failed: "
                            f"Source has checksum {source_checksum}, "
                            f"target has checksum {target_checksum}", Level.Warning)

                        self.exit_code = success = _MISMATCHED_CHECKSUM
                        raise _VerificationFailure()

                    # TODO Set metadata?

                except _VerificationFailure:
                    pass

            return bool(success)


@dataclass
class DependentTask:
    """ Model of sequence of task dependencies """
    task:Task
    dependency:T.Optional[DependentTask] = None

class BaseJob(T.Iterator[BaseAttempt], metaclass=ABCMeta):
    """
    Abstract base class for jobs

    Implementations required:
    * __init__     :: Any x String x Optional[Identifier] x Boolean -> None
    * __iadd__     :: DependentTask -> BaseJob
    * __next__     :: () -> BaseAttempt
    * max_attempts :: Getter () -> Optional[Integer] / Setter Integer -> None
    * status       :: Getter () -> BaseJobStatus
    * metadata     :: Getter () -> Dict[String, String]
    * set_metadata :: kwargs -> None
    """
    _client_id:str
    _job_id:T.Identifier

    @abstractmethod
    def __init__(self, state:T.Any, *, client_id:str, job_id:T.Optional[T.Identifier] = None, force_restart:bool = False) -> None:
        """
        Constructor

        @param  state          Object that describes the persisted state
        @param  client_id      Client identifier
        @param  job_id         Job ID for a running job
        @param  force_restart  Whether to forcibly resume a job
        """

    @abstractmethod
    def __iadd__(self, task:DependentTask) -> BaseJob:
        """ Add a sequence of dependent tasks to the job """

    def __iter__(self) -> BaseJob:
        return self

    @abstractmethod
    def __next__(self) -> BaseAttempt:
        """ Fetch the next pending permissible task to attempt """

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

    @property
    @abstractmethod
    def metadata(self) -> T.Dict[str, str]:
        """ Get the client metadata """

    @abstractmethod
    def set_metadata(self, **metadata:str) -> None:
        """ Set (insert/update) key-value client metadata """
