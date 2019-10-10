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
from common.models.task import Task
from common.exceptions import NOT_IMPLEMENTED


class DataNotReady(BaseException):
    """ Raised when data is not yet available """

class NoCommonChecksum(BaseException):
    """ Raised when two data objects do not have a common checksum """


Identifier = T.Any

class BaseJob(T.Iterable[Task], metaclass=ABCMeta):
    """
    Abstract base class for jobs

    Implementations required:
    * __init__ :: Any x Optional[Identifier] x bool -> None
    * __iadd__ :: Task -> BaseJob
    """
    _job_id:Identifier
    _max_attempts:int
    _max_concurrency:int

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

    @abstractmethod
    def __iadd__(self, task:Task) -> BaseJob:
        """ Add a task to the job """

    # TODO
    # * Method to fetch the next pending task
    # * Property to fetch the status (pending, running, succeeded, failed)
    # * etc...?
