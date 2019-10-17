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

from dataclasses import dataclass
from signal import SIGTERM
import os
import re
import shlex
import subprocess

from common import types as T
from common.exceptions import NOT_IMPLEMENTED
from .types import BaseSubmissionOptions, BaseExecutor, BaseWorkerStatus, \
                   CouldNotSubmit, NoSuchWorker, CouldNotAddressWorker, NotAWorker, \
                   WorkerIdentifier


def _lsf_job_id(identifier:WorkerIdentifier) -> str:
    """ Render a worker identifier as an LSF job ID """
    job_id = str(identifier.job)

    if identifier.worker is not None:
        job_id += f"[{identifier.worker}]"

    return job_id

def _run(command:str) -> subprocess.CompletedProcess:
    """ Wrapper for running commands """
    return subprocess.run(
        shlex.split(command),
        capture_output=True, check=False, text=True)


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
        # TODO Log unrecognised status
        return LSFWorkerStatus.Unknown

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
        # NOTE This sends a specific signal, rather than the default
        # (non-parametrised) invocation of bkill
        job_id = _lsf_job_id(worker)
        bkill  = _run(f"bkill -s {signum} {job_id}")

        if bkill.returncode != 0 or bkill.stderr is not None:
            if "No matching job found" in bkill.stderr:
                raise NoSuchWorker(f"No such LSF job: {job_id}")

            # TODO Log stderr
            raise CouldNotAddressWorker(f"Could not address LSF job {job_id}")

    @property
    def worker_id(self) -> WorkerIdentifier:
        job_id   = os.getenv("LSB_JOBID")
        index_id = int(os.getenv("LSB_JOBINDEX", "0")) or None

        if job_id is None:
            raise NotAWorker("Not running as an LSF job")

        return WorkerIdentifier(job_id, index_id)

    def worker_status(self, worker:T.Optional[WorkerIdentifier] = None) -> LSFWorkerStatus:
        # Get our own status, if not specified
        if worker is None:
            worker = self.worker_id

        job_id = _lsf_job_id(worker)
        bjobs  = _run(f"bjobs -noheader -o stat {job_id}")

        if bjobs.returncode != 0 or bjobs.stderr is not None:
            if "not found" in bjobs.stderr:
                raise NoSuchWorker(f"No such LSF job: {job_id}")

            # TODO Log stderr
            raise CouldNotAddressWorker(f"Could not address LSF job {job_id}")

        return LSFWorkerStatus(bjobs.stdout)
