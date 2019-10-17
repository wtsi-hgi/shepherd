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

from abc import ABCMeta, abstractmethod
from dataclasses import dataclass
from enum import Enum
from signal import SIGTERM

from common import types as T


class CouldNotSubmit(BaseException):
    """ Raised when a submission failed """

class NoSuchWorker(BaseException):
    """ Raised when a worker cannot be dereferenced """

class CouldNotAddressWorker(BaseException):
    """ Raised when a worker cannot be addressed """

class NotAWorker(BaseException):
    """ Raised when worker-specific invocations are made against non-workers """


class BaseWorkerStatus(Enum):
    """
    Abstract base job status

    Implementations required:
    * is_running    :: () -> bool
    * is_pending    :: () -> bool
    * is_done       :: () -> bool
    * is_successful :: () -> bool
    """
    @property
    @abstractmethod
    def is_running(self) -> bool:
        """ Is the worker running? """

    @property
    @abstractmethod
    def is_pending(self) -> bool:
        """ Is the worker pending? """

    @property
    @abstractmethod
    def is_done(self) -> bool:
        """ Has the worker finished? """

    @property
    @abstractmethod
    def is_successful(self) -> bool:
        """ Has a finished worker succeeded? """


@dataclass
class BaseSubmissionOptions:
    """ Base resource model """
    cores:int
    memory:int


@dataclass
class WorkerIdentifier:
    job:T.Identifier
    worker:T.Optional[int] = None


class BaseExecutor(T.Named, metaclass=ABCMeta):
    """
    Abstract base executor class

    Implementations required:
    * submit        :: <lots> -> List[WorkerIdentifier]
    * signal        :: WorkerIdentifier x int -> None
    * worker_id     :: () -> WorkerIdentifier
    * worker_status :: worker -> BaseWorkerStatus
    """
    @abstractmethod
    def submit(self, command:str, *, \
                     options:BaseSubmissionOptions, \
                     workers:int = 1, \
                     stdout:T.Optional[T.Path] = None, \
                     stderr:T.Optional[T.Path] = None, \
                     env:T.Optional[T.Dict[str, str]] = None) -> T.List[WorkerIdentifier]:
        """
        Submit a command to the executor

        @param   command  Command to execute
        @param   options  Submission options for the executor
        @param   workers  Number of workers (default: 1)
        @param   stdout   File to where to redirect stdout (optional)
        @param   stderr   File to where to redirect stderr (optional)
        @param   env      Environment variables for execution (optional)
        @return  List of worker identifiers
        """

    @abstractmethod
    def signal(self, worker:WorkerIdentifier, signum:int = SIGTERM) -> None:
        """
        Send a signal to a worker

        @param  worker  Worker identifier
        @param  signum  Signal number (default: SIGTERM)
        """

    @property
    @abstractmethod
    def worker_id(self) -> WorkerIdentifier:
        """ Get the current worker identifier """

    @abstractmethod
    def worker_status(self, worker:T.Optional[WorkerIdentifier] = None) -> BaseWorkerStatus:
        """
        Get the status of a worker

        @param   worker  Worker identifier
        @return  Worker status
        """
