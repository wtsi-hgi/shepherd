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
import re

from common import types as T
from common.logging import failure
from . import utils, queue
from .context import LSFWorkerContext
from .queue import LSFQueue
from ..exceptions import *
from ..types import BaseSubmissionOptions, BaseExecutor, Job, WorkerIdentifier


# Map convenience fields to LSF flags
_MAPPING = {
    "cores":  "n",
    "memory": "M",
    "queue":  "q",
    "group":  "G"}

def _args_to_lsf(arguments:T.Dict[str, T.Any]) -> str:
    """ Helper to generate LSF-style command line options """
    return " ".join(
        f"-{_MAPPING.get(arg, arg)} \"{val}\""
        for arg, val in arguments.items()
        if val is not None)


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


_JOB_ID = re.compile(r"(?<=Job <)\d+(?=>)")

class LSF(BaseExecutor):
    """ Platform LSF executor """
    _queues:T.Dict[str, LSFQueue]

    def __init__(self, config_dir:T.Path, name:str = "LSF") -> None:
        self._name = name
        self._queues = {
            q.name: q
            for q in queue.parse_config(config_dir / "lsb.queues")}

    def submit(self, job:Job, options:LSFSubmissionOptions) -> T.Iterator[WorkerIdentifier]:
        if options.queue is not None and options.queue not in self._queues:
            raise SubmissionException(f"No such LSF queue \"{options.queue}\"")

        extra_args:T.Dict[str, T.Any] = {
            **({"o": job.stdout.resolve()} if job.stdout is not None else {}),
            **({"e": job.stderr.resolve()} if job.stderr is not None else {})}

        if job.workers is not None and job.workers > 1:
            extra_args["J"] = f"shepherd_worker[1-{job.workers}]"

        if job.specific_worker is not None:
            extra_args["J"] = f"shepherd_worker[{job.specific_worker}]"

        dependency_expr = " && ".join(
            f"ended({utils.lsf_job_id(job_id)})"
            for job_id in job.dependencies)

        if dependency_expr:
            extra_args["w"] = dependency_expr

        bsub = utils.run(f"bsub {options.args} {_args_to_lsf(extra_args)} {job.command}", env=job.env)

        if bsub.returncode != 0:
            failure("Could not submit job to LSF", bsub)
            raise CouldNotSubmit("Could not submit job to LSF")

        # TODO In Python 3.8, use the new walrus operator
        id_search = _JOB_ID.search(bsub.stdout)
        if id_search is None:
            failure("Could not submit job to LSF", bsub)
            raise CouldNotSubmit("Could not submit job to LSF")

        # Workers in LSF are elements of an array job, if we have >1
        job_id = id_search.group()
        worker_ids = range(1, job.workers + 1) if job.workers is not None else [job.specific_worker]
        for worker_id in worker_ids:
            yield WorkerIdentifier(job_id, worker_id)

    def signal(self, worker:WorkerIdentifier, signum:int = SIGTERM) -> None:
        # NOTE This sends a specific signal, rather than the default
        # (non-parametrised) behaviour of bkill as may be expected
        job_id = utils.lsf_job_id(worker)
        bkill  = utils.run(f"bkill -s {signum} {job_id}")

        if bkill.returncode != 0 or bkill.stderr is not None:
            if "No matching job found" in bkill.stderr:
                raise NoSuchWorker(f"No such LSF job: {job_id}")

            failure(f"Could not address LSF job {job_id}", bkill)
            raise CouldNotAddressWorker(f"Could not address LSF job {job_id}")

    @property
    def worker(self) -> LSFWorkerContext:
        return LSFWorkerContext(self)

    def queue(self, name:str) -> LSFQueue:
        """ Return the parsed queue configuration """
        return self._queues[name]
