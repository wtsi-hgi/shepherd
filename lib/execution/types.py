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
from enum import Enum
from signal import SIGTERM

from common import types as T


# NOTE (FIXME?) The BaseWorkerStatus and, to a lesser extent, Job
# classes bear a similarity to counterparts in lib.state.types. Am I
# missing an abstraction here?...


@dataclass
class WorkerIdentifier:
    """ Worker identifier model """
    job:T.Identifier
    worker:T.Optional[int] = None


class BaseWorkerStatus(Enum):
    """
    Abstract base worker status

    Implementations required:
    * is_running     :: () -> bool
    * is_pending     :: () -> bool
    * is_done        :: () -> bool
    * is_successful  :: () -> bool
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


class BaseWorkerLimit(Enum):
    """ Abstract base worker limit enumeration """


class BaseWorkerContext(metaclass=ABCMeta):
    """
    Base worker context model

    Implementations required:
    * id     :: () -> WorkerIdentifier
    * status :: () -> BaseWorkerStatus
    * limit  :: BaseWorkerLimit -> Any
    """
    @property
    @abstractmethod
    def id(self) -> WorkerIdentifier:
        """ Get the worker identifier """

    @property
    @abstractmethod
    def status(self) -> BaseWorkerStatus:
        """ Get the worker status """

    @abstractmethod
    def limit(self, limit:BaseWorkerLimit) -> T.Any:
        """ Get the specified worker limit """


@dataclass
class _BaseJob:
    """ Base job attributes """
    command:str
    stdout:T.Optional[T.Path]        = None
    stderr:T.Optional[T.Path]        = None
    env:T.Optional[T.Dict[str, str]] = None

class Job(_BaseJob):
    """ Job model """
    _dependencies:T.List[WorkerIdentifier]
    _specific_worker:bool
    _worker:int

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        self._dependencies = []
        self._specific_worker = False
        self._worker = 1

    def __iadd__(self, dependency:WorkerIdentifier) -> Job:
        """ Add dependency """
        self._dependencies.append(dependency)
        return self

    @property
    def dependencies(self) -> T.Iterator[WorkerIdentifier]:
        """ Generator of dependencies """
        for d in self._dependencies:
            yield d

    @property
    def workers(self) -> T.Optional[int]:
        return None if self._specific_worker else self._worker

    @workers.setter
    def workers(self, value:int) -> None:
        self._specific_worker = False
        self._worker = value

    @property
    def specific_worker(self) -> T.Optional[int]:
        return self._worker if self._specific_worker else None

    @specific_worker.setter
    def specific_worker(self, value:int) -> None:
        self._specific_worker = True
        self._worker = value


@dataclass
class BaseSubmissionOptions:
    """ Base resource model """
    cores:int
    memory:int


class BaseExecutor(T.Named, metaclass=ABCMeta):
    """
    Abstract base executor class

    Implementations required:
    * submit :: Job x BaseSubmissionOptions -> Iterator[WorkerIdentifier]
    * signal :: WorkerIdentifier x int -> None
    * worker :: () -> BaseWorkerContext
    """
    @abstractmethod
    def submit(self, job:Job, options:BaseSubmissionOptions) -> T.Iterator[WorkerIdentifier]:
        """
        Submit a job to the executor

        @param   job      Job to execute
        @param   options  Submission options for the executor
        @return  Generator of worker identifiers
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
    def worker(self) -> BaseWorkerContext:
        """ Get the current worker context, if applicable """
