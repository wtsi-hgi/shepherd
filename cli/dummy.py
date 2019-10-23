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

from time import sleep
import sys

from common import types as T
from common.logging import log, Level
from common.models.filesystems import POSIXFilesystem, iRODSFilesystem
from lib.planning.transformers import strip_common_prefix, prefix, telemetry, debugging
from lib.planning.route_factories import posix_to_irods_factory
from lib.state.types import JobStatus, DataNotReady, WorkerRedundant
from lib.state.native import NativeJob, create_root
from lib.execution.lsf import LSF, LSFSubmissionOptions


_BINARY = T.Path(sys.argv[0]).resolve()

_FS = {
    "Lustre": POSIXFilesystem(name="Lustre", max_concurrency=50),
    "iRODS":  iRODSFilesystem(name="iRODS")
}

_EXEC = {
    # "farm3": LSF(T.Path("/usr/local/lsf/conf/lsbatch/farm3/configdir"), name="farm3")
    "farm4": LSF(T.Path("/usr/local/lsf/conf/lsbatch/farm4/configdir"), name="farm4")
    # "farm5": LSF(T.Path("/usr/local/lsf/conf/lsbatch/farm5/configdir"), name="farm5")
}

_CLUSTER = "farm4"


def _print_status(status:JobStatus) -> None:
    log(f"* Pending:   {status.pending}")
    log(f"* Running:   {status.running}")
    log(f"* Failed:    {status.failed}")
    log(f"* Succeeded: {status.succeeded}")


def main(*args:str) -> None:
    mode, *mode_args = args

    delegate = {
        "submit": submit,
        "__fofn": prepare_state_from_fofn,
        "__exec": run_state}

    if mode not in delegate:
        user_modes = ", ".join(mode for mode in delegate if not mode.startswith("__"))
        log(f"No such mode \"{mode}\"", Level.Critical)
        log(f"Valid user modes: {user_modes}")
        exit(1)

    delegate[mode](*mode_args)


def submit(fofn:str, prefix:str) -> None:
    # Create working directory
    state_root = create_root(T.Path("."))

    lsf = _EXEC[_CLUSTER]
    lsf_options = LSFSubmissionOptions(
        cores  = 1,
        memory = 1000,
        group  = "hgi",
        queue  = "normal")

    log_file = state_root / "prep.log"
    prep, *_ = lsf.submit(
        f"\"{_BINARY}\" __fofn \"{state_root}\" \"{fofn}\" \"{prefix}\"",
        options = lsf_options,
        stdout  = log_file,
        stderr  = log_file)

    log(f"Preparation job submitted with ID {prep.job}")
    log(f"State and logs will reside in {state_root}")


def prepare_state_from_fofn(state_root:str, fofn:str, subcollection:str) -> None:
    """ Lustre to iRODS state from FoFN """
    transfer = posix_to_irods_factory(_FS["Lustre"], _FS["iRODS"])
    transfer += strip_common_prefix
    transfer += prefix(T.Path(f"/humgen/shepherd_testing/{subcollection}"))
    transfer += debugging
    transfer += telemetry

    job = NativeJob(T.Path(state_root))
    job.filesystem_mapping = _FS

    # FIXME the maximum concurrency is a property of the filesystems
    # involved in a task, rather than that of an entire job...
    job.max_concurrency = min(fs.max_concurrency for fs in _FS.values())

    log(f"State:            {state_root}")
    log(f"Job ID:           {job.job_id}")
    log(f"Max. Attempts:    {job.max_attempts}")
    log(f"Max. Concurrency: {job.max_concurrency}")

    tasks = 0
    files = _FS["Lustre"]._identify_by_fofn(T.Path(fofn))
    for task in transfer.plan(files):
        log(("=" if tasks == 0 else "-") * 72)

        job += task
        tasks += 1

        log(f"Source: {task.source.filesystem} {task.source.address}")
        log(f"Target: {task.target.filesystem} {task.target.address}")

    log("=" * 72)
    log(f"Added {tasks} tasks to job:")
    _print_status(job.status)

    lsf = _EXEC[_CLUSTER]
    lsf_options = LSFSubmissionOptions(
        cores  = 4,
        memory = 1000,
        group  = "hgi",
        queue  = "long")

    log_file = T.Path(state_root) / "run.%I.log"
    runners = lsf.submit(
        f"\"{_BINARY}\" __exec \"{state_root}\" \"{job.job_id}\"",
        workers = job.max_concurrency,
        options = lsf_options,
        stdout  = log_file,
        stderr  = log_file)

    log(f"Execution job submitted with ID {runners[0].job} and {len(runners)} workers")


def run_state(state_root:str, job_id:str) -> None:
    """ Run through tasks in state database """
    job = NativeJob(T.Path(state_root), job_id=int(job_id), force_restart=True)
    job.filesystem_mapping = _FS

    log(f"State:         {state_root}")
    log(f"Job ID:        {job.job_id}")
    log(f"Max. Attempts: {job.max_attempts}")

    lsf = _EXEC[_CLUSTER]
    job.worker_index = worker_index = lsf.worker_id.worker
    log(f"Worker:        {worker_index} of {job.max_concurrency}")

    try:
        _print_status(job.status)

    except WorkerRedundant:
        log("Worker has nothing to do; terminating")
        exit(0)

    tasks = 0
    while job.status:
        try:
            task = next(job)

        except StopIteration:
            break

        except DataNotReady:
            log("Not ready; sleeping...")
            sleep(60)
            continue

        log(("=" if tasks == 0 else "-") * 72)
        log(f"Source: {task.source.filesystem} {task.source.address}")
        log(f"Target: {task.target.filesystem} {task.target.address}")

        # TODO Check exit status and update state
        task()

        tasks += 1

    log("=" * 72)
    log("Done")
    _print_status(job.status)
