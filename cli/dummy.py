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

import os
import sys
from time import sleep

from common import types as T
from common.logging import log
from common.models.filesystems import POSIXFilesystem, iRODSFilesystem
from lib import __version__ as lib_version
from lib.execution import types as Exec
from lib.execution.lsf import LSF, LSFSubmissionOptions
from lib.planning.route_factories import posix_to_irods_factory
from lib.planning.transformers import strip_common_prefix, prefix, telemetry, debugging
from lib.state import postgresql as State
from lib.state.exceptions import DataException
from lib.state.types import JobPhase, DependentTask


_CLIENT = "dummy"

_BINARY = T.Path(sys.argv[0]).resolve()

_FILESYSTEMS = (
    POSIXFilesystem(name="Lustre", max_concurrency=50),
    iRODSFilesystem(name="iRODS",  max_concurrency=10)
)

# These are lambdas because we haven't, at this point, checked the
# necessary environment variables are set
_GET_EXECUTOR = lambda: LSF(T.Path(os.environ["LSF_CONFIG"]))
_GET_STATE = lambda: State.PostgreSQL(
    database = os.environ["PG_DATABASE"],
    user     = os.environ["PG_USERNAME"],
    password = os.environ["PG_PASSWORD"],
    host     = os.environ["PG_HOST"],
    port     = int(os.getenv("PG_PORT", "5432")))

_LOG_HEADER = lambda: log.info(f"Shepherd: {_CLIENT} / lib {lib_version}")


def main(*args:str) -> None:
    # Log everything to standard output streams
    log.to_tty()

    # Expected environment variables (commented out entries are
    # optional, listed for documentation's sake)
    envvars = {
        "PG_HOST":        "PostgreSQL hostname",
        # "PG_PORT":      "PostgreSQL port [5432]",
        "PG_DATABASE":    "PostgreSQL database name",
        "PG_USERNAME":    "PostgreSQL username",
        "PG_PASSWORD":    "PostgreSQL password",
        "LSF_CONFIG":     "Path to LSF cluster configuration directory",
        "LSF_GROUP":      "LSF Fairshare group to run under",
        "PREP_QUEUE":     "LSF queue to use for the preparation phase",
        "TRANSFER_QUEUE": "LSF queue to use for the transfer phase"
        # "MAX_ATTEMPTS": "Maximum attempts per transfer task [3]"
        # "SHEPHERD_LOG": "Logging directory [pwd]"
    }

    # Mode delegation routines
    delegate = {
        "submit":     submit,
        "status":     status,
        "__prepare":  prepare,
        "__transfer": transfer
    }

    # Check the appropriate environment variables are set to connect to
    # the PostgreSQL database, otherwise bail out
    if any(env not in os.environ for env in envvars):
        log.critical("Incomplete environment variables")

        column_width = max(map(len, envvars))
        for env, desc in envvars.items():
            log.info(f"* {env:{column_width}}  {desc}")

        sys.exit(1)

    # Check the binary is running in a known mode, otherwise bail out
    mode, *mode_args = args
    if mode not in delegate:
        log.critical(f"No such mode \"{mode}\"")

        user_modes = ", ".join(mode for mode in delegate if not mode.startswith("__"))
        log.info(f"Valid user modes: {user_modes}")

        sys.exit(1)

    # Delegate to appropriate mode
    delegate[mode](*mode_args)


def submit(fofn:str, subcollection:str) -> None:
    """ Submit a FoFN job to the executioner """
    # Set logging directory, if not already
    if "SHEPHERD_LOG" not in os.environ:
        os.environ["SHEPHERD_LOG"] = str(T.Path(".").resolve())

    log_dir = T.Path(os.environ["SHEPHERD_LOG"]).resolve()
    log.to_file(log_dir / "submit.log")

    fofn_path = T.Path(fofn).resolve()

    _LOG_HEADER()
    log.info(f"Logging to {log_dir}")
    log.info(f"Will transfer contents of {fofn_path}")

    state = _GET_STATE()
    job = State.Job(state, client_id=_CLIENT)
    job.max_attempts = max_attempts = int(os.getenv("MAX_ATTEMPTS", "3"))
    job.set_metadata(fofn=str(fofn_path), subcollection=subcollection)

    log.info(f"Created new job with ID {job.job_id}, with up to {max_attempts} attempts per task")

    executor = _GET_EXECUTOR()

    prep_options = LSFSubmissionOptions(
        cores  = 1,
        memory = 1000,
        group  = os.environ["LSF_GROUP"],
        queue  = os.environ["PREP_QUEUE"]
    )

    prep_worker = Exec.Job(f"\"{_BINARY}\" __prepare {job.job_id}")
    prep_worker.stdout = prep_worker.stderr = log_dir / "prep.log"
    prep_runner, *_ = executor.submit(prep_worker, prep_options)

    log.info(f"Preparation phase submitted with LSF ID {prep_runner.job}")

    # NOTE We're only dealing with the Lustre-iRODS tuple, so this is
    # simplified considerably. In a multi-route context, the maximum
    # concurrency should be a function of the pairwise minimum of
    # filesystems' maximum concurrencies for each stage of the route.
    # That function could be, e.g.: max for maximum speed, but also
    # maximum waste (in terms of redundant workers); min (or some lower
    # constant) for zero wastage, but longer flight times. The
    # arithmetic mean would probably be a good thing to go for, without
    # implementing complicated dynamic load handling...
    max_concurrency = min(fs.max_concurrency for fs in _FILESYSTEMS)

    transfer_options = LSFSubmissionOptions(
        cores  = 4,
        memory = 1000,
        group  = os.environ["LSF_GROUP"],
        queue  = os.environ["TRANSFER_QUEUE"]
    )

    transfer_worker = Exec.Job(f"\"{_BINARY}\" __transfer {job.job_id}")
    transfer_worker.workers = max_concurrency
    transfer_worker.stdout = transfer_worker.stderr = log_dir / "transfer.%I.log"
    transfer_runners = transfer_runner, *_ = executor.submit(transfer_worker, transfer_options)

    log.info(f"Transfer phase submitted with LSF ID {transfer_runner.job} and {len(transfer_runners)} workers")


def prepare(job_id:str) -> None:
    """ Prepare the Lustre to iRODS task from FoFN """
    _LOG_HEADER()

    state = _GET_STATE()
    job = State.Job(state, client_id=_CLIENT, job_id=int(job_id))

    # Get the FoFN path and prefix from the client metadata
    fofn = T.Path(job.metadata.fofn)
    subcollection = job.metadata.subcollection

    if job.status.phase(JobPhase.Preparation).start is not None:
        raise DataException(f"Preparation phase has already started for job {job.job_id}")

    with job.status.phase(JobPhase.Preparation):
        log.info("Preparation phase started")

        # Setup the transfer route
        route = posix_to_irods_factory(*_FILESYSTEMS)
        route += strip_common_prefix
        route += prefix(T.Path(f"/humgen/shepherd_testing/{subcollection}"))
        route += debugging
        route += telemetry

        tasks = 0
        lustre, *_ = _FILESYSTEMS
        files = lustre._identify_by_fofn(fofn)
        for task in route.plan(files):
            log.info(f"{task.source.address} on {task.source.filesystem} to "
                     f"{task.target.address} on {task.target.filesystem}")

            # NOTE With just one step in our route, we have no
            # inter-task dependencies
            job += DependentTask(task)
            tasks += 1

        log.info(f"Added {tasks} tasks to the job")

    log.info("Preparation phase complete")


def transfer(job_id:str) -> None:
    ...


def status(job_id:str) -> None:
    ...


# def run_state(state_root:str, job_id:str) -> None:
#     """ Run through tasks in state database """
#     job = NativeJob(T.Path(state_root), job_id=int(job_id), force_restart=True)
#     job.filesystem_mapping = _FS
#
#     log(f"State:         {state_root}")
#     log(f"Job ID:        {job.job_id}")
#     log(f"Max. Attempts: {job.max_attempts}")
#
#     lsf = _EXEC[_CLUSTER]
#     job.worker_index = worker_index = lsf.worker.id.worker
#     log(f"Worker:        {worker_index} of {job.max_concurrency}")
#
#     try:
#         _print_status(job.status)
#
#     except WorkerRedundant:
#         log("Worker has nothing to do; terminating")
#         exit(0)
#
#     tasks = 0
#     while job.status:
#         try:
#             task = next(job)
#
#         except StopIteration:
#             break
#
#         except DataNotReady:
#             log("Not ready; sleeping...")
#             sleep(60)
#             continue
#
#         log(("=" if tasks == 0 else "-") * 72)
#         log(f"Source: {task.source.filesystem} {task.source.address}")
#         log(f"Target: {task.target.filesystem} {task.target.address}")
#
#         # TODO Check exit status and update state
#         task()
#
#         tasks += 1
#
#     log("=" * 72)
#     log("Done")
#     _print_status(job.status)
