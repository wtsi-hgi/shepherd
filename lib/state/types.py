"""
Copyright (c) 2019, 2020 Genome Research Limited

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
from concurrent.futures import ThreadPoolExecutor
from contextlib import AbstractContextManager

from common import types as T, time
from common.logging import log
from common.models.task import ExitCode, Task
from common.models.filesystems.types import BaseFilesystem
from .exceptions import NoCommonChecksumAlgorithm, PeriodNotStarted, NoTasksAvailable


class BaseStateProtocol(metaclass=ABCMeta):
    """
    Abstract base class for encapsulating communication with the
    underlying persistence engine (e.g., managing connections,
    performing queries, etc.) and application-level classes

    NOTE Must be thread-safe

    Implementations required:
    * filesystem_convertor :: *args x **kwargs -> BaseFilesystem
    """
    _filesystems:T.Dict[str, BaseFilesystem]

    def register_filesystems(self, *filesystems:BaseFilesystem) -> None:
        # Add filesystem instances to our internal mapping
        if not hasattr(self, "_filesystems"):
            self._filesystems = {}

        self._filesystems = {
            **self._filesystems,
            **{fs.name: fs for fs in filesystems}
        }

    @abstractmethod
    def filesystem_convertor(self, *args, **kwargs) -> BaseFilesystem:
        """
        Convert a persisted representation of a filesystem into its
        respective instance

        @param   *args, **kwargs        Necessary data from the
                                        persistence engine to map to the
                                        appropriate filesystem instance
        @raises  NoFilesystemConvertor  Ambiguous conversion
        @return  Filesystem instance
        """


FORCIBLY_TERMINATED  = ExitCode(-3)

# Verification failure handling
_MISMATCHED_SIZE     = ExitCode(-1)
_MISMATCHED_CHECKSUM = ExitCode(-2)

class _VerificationFailure(Exception):
    """ Raised on verification failure """


@dataclass
class _DurationMixin:
    """ Model for durations """
    start:T.Optional[T.DateTime]
    finish:T.Optional[T.DateTime]

    @property
    def runtime(self) -> T.TimeDelta:
        """ Phase runtime """
        if self.start is None:
            raise PeriodNotStarted("Period has yet to start")

        until = self.finish or time.now()
        return until - self.start

    @property
    def complete(self) -> bool:
        # Truthy once there's a finish timestamp
        return self.finish is not None

class _BaseDurationMixin(_DurationMixin, AbstractContextManager, metaclass=ABCMeta):
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
        return False


@dataclass(frozen=True)
class JobThroughput:
    """ Model of throughput rates between filesystems """
    transfer_rate:float  # bytes/second
    failure_rate:float   # Probability


class JobPhase(Enum):
    """ Enumeration of job phases """
    Preparation = auto()
    Transfer    = auto()

# Convenience aliases
_PREPARATION = JobPhase.Preparation
_TRANSFER    = JobPhase.Transfer


class BasePhaseStatus(_BaseDurationMixin, metaclass=ABCMeta):
    """ Abstract base class for phase status """
    def __bool__(self) -> bool:
        return not self.complete


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
        return self.phase(_PREPARATION) or self.pending > 0

    @abstractmethod
    def throughput(self, source:BaseFilesystem, target:BaseFilesystem) -> JobThroughput:
        """
        Return the throughput rates between the given filesystems

        @param   source            Source filesystem
        @param   target            Target filesystem
        @raise   NoThroughputData  No throughput data available
        @return  Job throughput
        """

    @abstractmethod
    def phase(self, phase:JobPhase) -> BasePhaseStatus:
        """
        Return the phase status for the specified phase

        @param   phase             Job phase
        @raise   PeriodNotStarted  Phase has yet to start
        @return  Phase status
        """

    @property
    def complete(self) -> bool:
        # Truthy when the transfer phase has ended (n.b., which is
        # dependent on the preparation phase ending)
        # NOTE Completion does not imply success!
        try:
            return not self.phase(_TRANSFER)

        except PeriodNotStarted:
            return False


class DataOrigin(Enum):
    """ Enumeration of data origins """
    Source = auto()
    Target = auto()

# Convenience aliases
_SOURCE = DataOrigin.Source
_TARGET = DataOrigin.Target


@dataclass
class _AttemptMixin:
    """ Model of task attempts """
    task:Task

class BaseAttempt(_AttemptMixin, _BaseDurationMixin, metaclass=ABCMeta):
    """
    Abstract base class for task attempts

    Implementations required:
    * size      :: DataOrigin -> int         Must be thread-safe!
    * checksum  :: DataOrigin x str -> int   Must be thread-safe!
    * exit_code :: Getter () -> ExitCode / Setter ExitCode -> None
    """
    @abstractmethod
    def size(self, origin:DataOrigin) -> int:
        """
        Persist and return the origin's data size

        NOTE Implementation must be thread-safe
        """

    @abstractmethod
    def checksum(self, origin:DataOrigin, algorithm:str) -> str:
        """
        Persist and return the origin's data checksum

        NOTE Implementation must be thread-safe
        """

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

    def _get_source_properties(self, *algorithms:str) -> T.Tuple[int, T.Dict[str, str]]:
        # Fetch the source data's size and checksums using the specified
        # algorithms (to be run internally in a separate thread)
        return self.size(_SOURCE), \
               {algorithm: self.checksum(_SOURCE, algorithm) for algorithm in algorithms}

    def __call__(self) -> bool:
        """ Attempt the transfer task """
        # The context manager sets the attempt start and finish timestamps
        with self:
            task = self.task
            log.info(f"Attempting transfer of "
                     f"{task.source.address} on {task.source.filesystem} to "
                     f"{task.target.address} on {task.target.filesystem}")

            with ThreadPoolExecutor(max_workers=1) as executor:
                # TODO Different/multiple checksum algorithms
                properties = executor.submit(self._get_source_properties, "md5")

                # Run task in main tread and join on properties thread
                success = self.task()
                source_size, source_checksums = properties.result()

            if not success:
                log.warning(f"Attempt failed with exit code {success.exit_code}")

            else:
                log.info(f"Data copied; verifying...")

                try:
                    target_size = self.size(_TARGET)
                    if source_size != target_size:
                        log.warning(f"Attempt failed: "
                                    f"Source is {source_size} bytes; "
                                    f"target is {target_size} bytes")

                        success = _MISMATCHED_SIZE
                        raise _VerificationFailure()

                    # TODO Different/multiple checksum algorithms
                    source_checksum = source_checksums["md5"]
                    target_checksum = self.checksum(_TARGET, "md5")
                    if source_checksum != target_checksum:
                        log.warning(f"Attempt failed: "
                                    f"Source has checksum {source_checksum}; "
                                    f"target has checksum {target_checksum}")

                        success = _MISMATCHED_CHECKSUM
                        raise _VerificationFailure()

                    # TODO Data metadata: There is no need to set this
                    # for each intermediary stage; just set the final
                    # target metadata to that of the original source.
                    # Think about how to implement this...

                except _VerificationFailure:
                    pass

            self.exit_code = success
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
    * attempt      :: Optional[TimeDelta] -> BaseAttempt
    * max_attempts :: Getter () -> Optional[Integer] / Setter Integer -> None
    * status       :: Getter () -> BaseJobStatus
    * metadata     :: Getter () -> SimpleNamespace
    * set_metadata :: kwargs -> None
    """
    _client_id:str
    _job_id:T.Identifier

    @abstractmethod
    def __init__(self, state:BaseStateProtocol, *, client_id:str, job_id:T.Optional[T.Identifier] = None, force_restart:bool = False) -> None:
        """
        Constructor

        @param  state          Encapsulation of the persisted state
        @param  client_id      Client identifier
        @param  job_id         Job ID for a running job
        @param  force_restart  Whether to forcibly resume a job
        """

    @abstractmethod
    def __iadd__(self, task:DependentTask) -> BaseJob:
        """ Add a sequence of dependent tasks to the job """
        # NOTE The root source is assumed to exist, so we can query its
        # size. Ideally, this size should be persisted along with the
        # task, as THERE IS NO OTHER INTERFACE FOR DOING SO. Without it,
        # it would be impossible to make transfer rate calculations.

    def __iter__(self) -> BaseJob:
        return self

    def __next__(self) -> BaseAttempt:
        # The next pending task to attempt, *regardless* of time limit
        try:
            return self.attempt()

        except NoTasksAvailable:
            if self.status.complete:
                raise StopIteration()

    @property
    def job_id(self) -> T.Identifier:
        return self._job_id

    @abstractmethod
    def attempt(self, time_limit:T.Optional[T.TimeDelta] = None) -> BaseAttempt:
        """
        Fetch the next pending task to attempt, given the optional time
        limit (which is ignored, if no timing statistics are available)

        @param   time_limit  Only fetch attempts that are predicted to
                             complete within this duration (optional;
                             defaults to None, to ignore time limits)
        @return  Attempt
        """

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

    # TODO Make the client metadata interface nicer

    @property
    @abstractmethod
    def metadata(self) -> T.SimpleNamespace:
        """ Get the job metadata """

    @abstractmethod
    def set_metadata(self, **metadata:str) -> None:
        """ Set (insert/update) key-value metadata """
