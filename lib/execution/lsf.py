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

from dataclasses import asdict, dataclass
from signal import SIGTERM
import os
import re
import shlex
import subprocess

from common import types as T, time
from common.logging import Level, log, failure
from .exceptions import *
from .types import BaseSubmissionOptions, BaseExecutor, BaseWorkerContext, BaseWorkerStatus, \
                   WorkerIdentifier


def _lsf_job_id(identifier:WorkerIdentifier) -> str:
    """ Render a worker identifier as an LSF job ID """
    job_id = str(identifier.job)

    if identifier.worker is not None:
        job_id += f"[{identifier.worker}]"

    return job_id

def _args_to_lsf(arguments:T.Dict[str, T.Any]) -> str:
    """ Helper to generate LSF-style command line options """
    # Map convenience fields to LSF flags
    mapping = {
        "cores":  "n",
        "memory": "M",
        "queue":  "q",
        "group":  "G"}

    return " ".join(
        f"-{mapping.get(arg, arg)} \"{val}\""
        for arg, val in arguments.items()
        if val is not None)

def _run(command:str, *, env:T.Optional[T.Dict[str, str]] = None) -> subprocess.CompletedProcess:
    """ Wrapper for running commands """
    log(f"Running: {command}", Level.Debug)
    return subprocess.run(
        shlex.split(command), env=env,
        capture_output=True, check=False, text=True)


@dataclass
class LSFSubmissionOptions(BaseSubmissionOptions):
    # TODO This is currently pared down to what we're interested in
    queue:T.Optional[str]  = None
    group:T.Optional[str]  = None
    cwd:T.Optional[T.Path] = None

    @property
    def args(self) -> str:
        """ Generate the bsub arguments from submission options """
        return _args_to_lsf({
            **asdict(self),
            "R": f"span[hosts=1] select[mem>{self.memory}] rusage[mem={self.memory}]"})


@dataclass
class LSFQueue:
    name:str
    runlimit:T.Optional[T.TimeDelta] = None
    # TODO Other queue properties...

    @staticmethod
    def parse_config(config:T.Path) -> T.Dict[str, LSFQueue]:
        """
        Parse lsb.queues file to extract queue configuration

        @param   config  Path to lsb.queues
        @return  Dictionary of parsed queue configurations
        """
        comment = re.compile(r"^\s*#")
        begin   = re.compile(r"Begin Queue")
        end     = re.compile(r"End Queue")
        setting = re.compile(r"(?P<key>\w+)\s*=\s*(?P<value>.+)\s*$")

        queues = {}

        def _parse_runlimit(value:str) -> T.TimeDelta:
            if value.isnumeric():
                return time.delta(minutes=int(value))

            # TODO In Python 3.8, use the new walrus operator
            hhmm = re.search(r"(?P<hours>\d+):(?P<minutes>\d{2})", value)
            return time.delta(hours=int(hhmm["hours"]), minutes=int(hhmm["minutes"]))

        mapping = {
            "QUEUE_NAME": ("name",     None),
            "RUNLIMIT":   ("runlimit", _parse_runlimit)}

        in_queue_def = False
        this_queue:T.Dict[str, T.Any] = {}
        with config.open(mode="rt") as f:
            for line in f:
                if comment.match(line):
                    continue

                if begin.search(line):
                    in_queue_def = True
                    continue

                if end.search(line):
                    in_queue_def = False

                    name         = this_queue["name"]
                    queues[name] = LSFQueue(**this_queue)
                    this_queue   = {}

                    continue

                # TODO In Python 3.8, use the new walrus operator
                option = setting.search(line)
                if in_queue_def and option is not None:
                    key, value = option["key"], option["value"]

                    if key in mapping:
                        mapped_key, value_mapper = mapping[key]
                        this_queue[mapped_key] = (value_mapper or str)(value)

        return queues


@dataclass
class LSFWorkerContext(BaseWorkerContext):
    # FIXME This is a tight-coupling to LSF
    queue:LSFQueue


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

    _context:LSFWorkerContext

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
        return self == LSFWorkerStatus.Succeeded \
            or self == LSFWorkerStatus.Failed

    @property
    def is_successful(self) -> bool:
        return self == LSFWorkerStatus.Succeeded

    @property
    def context(self) -> LSFWorkerContext:
        return self._context

    @context.setter
    def context(self, value:LSFWorkerContext) -> None:
        self._context = value


_JOB_ID = re.compile(r"(?<=Job <)\d+(?=>)")

class LSF(BaseExecutor):
    """ Platform LSF executor """
    _queues:T.Dict[str, LSFQueue]

    def __init__(self, config_dir:T.Path, name:str = "LSF") -> None:
        self._name   = name
        self._queues = LSFQueue.parse_config(config_dir / "lsb.queues")

    def submit(self, command:str, *, \
                     options:LSFSubmissionOptions, \
                     workers:T.Optional[int]                           = 1, \
                     worker_index:T.Optional[int]                      = None, \
                     dependencies:T.Optional[T.List[WorkerIdentifier]] = None, \
                     stdout:T.Optional[T.Path]                         = None, \
                     stderr:T.Optional[T.Path]                         = None, \
                     env:T.Optional[T.Dict[str, str]]                  = None) -> T.List[WorkerIdentifier]:

        # Sanity check our input
        # TODO Change this from an assertion to raising a SubmissionException
        assert (workers is not None and workers > 0 and worker_index is None) \
            or (workers is None and worker_index is not None and worker_index > 1)

        if options.queue is not None:
            if options.queue not in self._queues:
                raise SubmissionException(f"No such LSF queue \"{options.queue}\"")

        extra_args:T.Dict[str, T.Any] = {
            **({"o": stdout.resolve()} if stdout is not None else {}),
            **({"e": stderr.resolve()} if stderr is not None else {})}

        if workers is not None and workers > 1:
            extra_args["J"] = f"shepherd_worker[1-{workers}]"

        if worker_index is not None:
            extra_args["J"] = f"shepherd_worker[{worker_index}]"

        if dependencies is not None:
            extra_args["w"] = " && ".join(f"ended({_lsf_job_id(job_id)})" for job_id in dependencies)

        bsub = _run(f"bsub {options.args} {_args_to_lsf(extra_args)} {command}", env=env)

        if bsub.returncode != 0:
            failure("Could not submit job to LSF", bsub)
            raise CouldNotSubmit("Could not submit job to LSF")

        id_search = _JOB_ID.search(bsub.stdout)
        if id_search is None:
            failure("Could not submit job to LSF", bsub)
            raise CouldNotSubmit("Could not submit job to LSF")

        # Workers in LSF are elements of an array job, if we have >1
        job_id = id_search.group()
        worker_ids = range(1, workers + 1) if workers is not None else [worker_index]
        return [WorkerIdentifier(job_id, worker_id) for worker_id in worker_ids]

    def signal(self, worker:WorkerIdentifier, signum:int = SIGTERM) -> None:
        # NOTE This sends a specific signal, rather than the default
        # (non-parametrised) behaviour of bkill as may be expected
        job_id = _lsf_job_id(worker)
        bkill  = _run(f"bkill -s {signum} {job_id}")

        if bkill.returncode != 0 or bkill.stderr is not None:
            if "No matching job found" in bkill.stderr:
                raise NoSuchWorker(f"No such LSF job: {job_id}")

            failure(f"Could not address LSF job {job_id}", bkill)
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
        bjobs  = _run(f"bjobs -noheader -o 'stat queue delimiter=\":\"' {job_id}")

        if bjobs.returncode != 0 or bjobs.stderr != "":
            if "not found" in bjobs.stderr:
                raise NoSuchWorker(f"No such LSF job: {job_id}")

            failure(f"Could not address LSF job {job_id}", bjobs)
            raise CouldNotAddressWorker(f"Could not address LSF job {job_id}")

        status, queue = bjobs.stdout.strip().split(":")
        worker_status = LSFWorkerStatus(status)
        worker_status.context = LSFWorkerContext(queue=self._queues[queue])

        return worker_status
