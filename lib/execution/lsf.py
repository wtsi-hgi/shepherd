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

from dataclasses import dataclass
from signal import SIGTERM
import os
import re

from common import types as T
from common.exceptions import NOT_IMPLEMENTED
from .types import BaseSubmissionOptions, BaseExecutor, BaseWorkerStatus, \
                   CouldNotSubmit, NoSuchWorker, CouldNotSignalWorker, NotAWorker, \
                   WorkerIdentifier


def _lsf_job_id(identifier:WorkerIdentifier) -> str:
    """ Render a worker identifier as an LSF job ID """
    job_id = str(identifier.job)

    if identifier.worker is not None:
        job_id += f"[{identifier.worker}]"

    return job_id


class LSFWorkerStatus(BaseWorkerStatus):
    Running          = "RUN"
    Pending          = "PEND"
    Succeeded        = "DONE"
    Failed           = "EXIT"
    UserSuspended    = "USUSP"
    SystemSuspended  = "SSUSP"
    PendingSuspended = "PSUSP"
    Unknown          = "UNKWN"
    Waiting          = "WAIT"
    Zombified        = "ZOMBI"

    @property
    def is_running(self) -> bool:
        return self == LSFWorkerStatus.Running

    @property
    def is_pending(self) -> bool:
        return self == LSFWorkerStatus.Pending

    @property
    def is_done(self) -> bool:
        return self == LSFWorkerStatus.Succeeded \
            or self == LSFWorkerStatus.Failed

    @property
    def is_successful(self) -> bool:
        return self == LSFWorkerStatus.Succeeded


@dataclass
class LSFSubmissionOptions(BaseSubmissionOptions):
    pass


class LSF(BaseExecutor):
    """ Platform LSF executor """
    def __init__(self, name:str) -> None:
        self._name = name

    def submit(self, command:str, *, \
                     options:LSFSubmissionOptions, \
                     workers:int = 1, \
                     stdout:T.Optional[T.Path] = None, \
                     stderr:T.Optional[T.Path] = None, \
                     env:T.Optional[T.Dict[str, str]] = None) -> T.List[WorkerIdentifier]:
        raise NOT_IMPLEMENTED

    def signal(self, worker:WorkerIdentifier, signum:int = SIGTERM) -> None:
        raise NOT_IMPLEMENTED

    @property
    def worker_id(self) -> WorkerIdentifier:
        job_id   = os.getenv("LSB_JOBID")
        index_id = int(os.getenv("LSB_JOBINDEX", "0")) or None

        if job_id is None:
            raise NotAWorker("Not running as an LSF job")

        return WorkerIdentifier(job_id, index_id)

    def worker_status(self, worker:WorkerIdentifier) -> LSFWorkerStatus:
        raise NOT_IMPLEMENTED
