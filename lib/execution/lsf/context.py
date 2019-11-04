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

import os

import lib.execution.lsf.executor as executor
from common import types as T
from common.logging import log, failure
from . import utils
from .queue import LSFQueue
from ..exceptions import *
from ..types import WorkerIdentifier, BaseWorkerStatus, BaseWorkerContext


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

    @classmethod
    def _missing_(cls:LSFWorkerStatus, value:str) -> LSFWorkerStatus:
        log(f"Unrecognised LSF status \"{value}\"; converting to UNKNOWN")
        return LSFWorkerStatus.Unknown

    @property
    def is_running(self) -> bool:
        return self == LSFWorkerStatus.Running

    @property
    def is_pending(self) -> bool:
        return self == LSFWorkerStatus.Pending

    @property
    def is_done(self) -> bool:
        return self in (LSFWorkerStatus.Succeeded, LSFWorkerStatus.Failed)

    @property
    def is_successful(self) -> bool:
        return self == LSFWorkerStatus.Succeeded


class LSFWorkerContext(BaseWorkerContext):
    """ Worker context """
    _lsf:executor.LSF
    _id:WorkerIdentifier

    def __init__(self, lsf:executor.LSF, worker_id:T.Optional[WorkerIdentifier] = None) -> None:
        self._lsf = lsf

        if worker_id is None:
            job_id   = os.getenv("LSB_JOBID")
            index_id = int(os.getenv("LSB_JOBINDEX", "0")) or None

            if job_id is None:
                raise NotAWorker("Not running as an LSF job")

            self._id = WorkerIdentifier(job_id, index_id)

    @property
    def id(self) -> WorkerIdentifier:
        return self._id

    @property
    def _bjobs(self) -> T.Tuple[LSFWorkerStatus, LSFQueue]:
        """ Get worker context """
        job_id = utils.lsf_job_id(self._id)
        bjobs  = utils.run(f"bjobs -noheader -o 'stat queue delimiter=\":\"' {job_id}")

        if bjobs.returncode != 0 or bjobs.stderr != "":
            if "not found" in bjobs.stderr:
                raise NoSuchWorker(f"No such LSF job: {job_id}")

            failure(f"Could not address LSF job {job_id}", bjobs)
            raise CouldNotAddressWorker(f"Could not address LSF job {job_id}")

        status, queue = bjobs.stdout.strip().split(":")
        return LSFWorkerStatus(status), self._lsf.queue(queue)

    @property
    def status(self) -> LSFWorkerStatus:
        status, _ = self._bjobs
        return status

    @property
    def queue(self) -> LSFQueue:
        """ Return the queue for the worker """
        _, queue = self._bjobs
        return queue
