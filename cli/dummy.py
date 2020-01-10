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
from math import ceil, log10
from signal import SIGTERM
from time import sleep

from cli import __version__ as cli_version
from common import types as T, time
from common.logging import log
from common.models.filesystems import POSIXFilesystem, iRODSFilesystem
from lib import __version__ as lib_version
from lib.execution import types as Exec
from lib.execution.lsf import LSF, LSFSubmissionOptions
from lib.execution.lsf.context import LSFWorkerLimit
from lib.planning.route_factories import posix_to_irods_factory
from lib.planning.transformers import strip_common_prefix, prefix, telemetry, debugging
from lib.state import postgresql as State
from lib.state.exceptions import DataException, NoThroughputData, NoTasksAvailable
from lib.state.types import JobPhase, DependentTask, DataOrigin


_CLIENT = "dummy"

_BINARY = T.Path(sys.argv[0]).resolve()

# Approximate start time for the process, plus a conservative threshold
_START_TIME = time.now()
_FUDGE_TIME = time.delta(minutes=5)

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

_LOG_HEADER = lambda: log.info(f"Shepherd: {_CLIENT} {cli_version} / lib {lib_version}")

# Convenience aliases
_PREPARE = JobPhase.Preparation
_TRANSFER = JobPhase.Transfer


def main(*args:str) -> None:
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
        "TRANSFER_QUEUE": "LSF queue to use for the transfer phase",
        "IRODS_BASE":     "Base iRODS collection into which to transfer"
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


def _transfer_worker(job_id:T.Identifier, logs:T.Path) -> T.Tuple[Exec.Job, LSFSubmissionOptions]:
    # Setup a consistent worker job
    worker = Exec.Job(f"\"{_BINARY}\" __transfer {job_id}")
    worker.stdout = worker.stderr = logs / "transfer.%I.log"

    options = LSFSubmissionOptions(
        cores  = 4,
        memory = 1000,
        group  = os.environ["LSF_GROUP"],
        queue  = os.environ["TRANSFER_QUEUE"]
    )

    return worker, options


_SI  = ["", "k",  "M",  "G",  "T",  "P"]
_IEC = ["", "Ki", "Mi", "Gi", "Ti", "Pi"]

def _human_size(value:float, base:int = 1024, threshold:float = 0.8) -> str:
    """ Quick-and-dirty size quantifier """
    quantifiers = _IEC if base == 1024 else _SI
    sigfigs = ceil(log10(base * threshold))

    order = 0
    while order < len(quantifiers) - 1 and value > base * threshold:
        value /= base
        order += 1

    return f"{value:.{sigfigs}g} {quantifiers[order]}"


def submit(fofn:str, subcollection:str) -> None:
    """ Submit a FoFN job to the executioner """
    # Set logging directory, if not already
    if "SHEPHERD_LOG" not in os.environ:
        os.environ["SHEPHERD_LOG"] = str(T.Path(".").resolve())

    log_dir = T.Path(os.environ["SHEPHERD_LOG"]).resolve()
    log.to_file(log_dir / "submit.log")

    fofn_path = T.Path(fofn).resolve()
    irods_base = os.environ["IRODS_BASE"]

    _LOG_HEADER()
    log.info(f"Logging to {log_dir}")
    log.info(f"Will transfer contents of {fofn_path} to {irods_base}/{subcollection}")

    state = _GET_STATE()
    job = State.Job(state, client_id=_CLIENT)
    job.max_attempts = max_attempts = int(os.getenv("MAX_ATTEMPTS", "3"))
    job.set_metadata(fofn          = str(fofn_path),
                     irods_base    = irods_base,
                     subcollection = subcollection,
                     logs          = str(log_dir),
                     DAISYCHAIN    = "Yes")         # NOTE For debugging

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

    transfer_worker, transfer_options = _transfer_worker(job.job_id, log_dir)
    transfer_worker.workers = max_concurrency
    transfer_runners = transfer_runner, *_ = executor.submit(transfer_worker, transfer_options)

    log.info(f"Transfer phase submitted with LSF ID {transfer_runner.job} and {len(list(transfer_runners))} workers")


def prepare(job_id:str) -> None:
    """ Prepare the Lustre to iRODS task from FoFN """
    _LOG_HEADER()

    state = _GET_STATE()
    job = State.Job(state, client_id=_CLIENT, job_id=int(job_id))

    # Get the FoFN path and prefix from the client metadata
    fofn = T.Path(job.metadata.fofn)
    irods_base = T.Path(job.metadata.irods_base)
    subcollection = job.metadata.subcollection

    if job.status.phase(_PREPARE).start is not None:
        raise DataException(f"Preparation phase has already started for job {job.job_id}")

    with job.status.phase(_PREPARE):
        log.info("Preparation phase started")

        # Setup the transfer route
        route = posix_to_irods_factory(*_FILESYSTEMS)
        route += strip_common_prefix
        route += prefix(irods_base / subcollection)
        route += debugging
        route += telemetry

        tasks = 0
        lustre, *_ = _FILESYSTEMS
        files = lustre._identify_by_fofn(fofn)
        for task in route.plan(files):
            log.info(f"{task.source.address} on {task.source.filesystem} to "
                     f"{task.target.address} on {task.target.filesystem}")

            # NOTE With just one step in our route, we have no
            # inter-task dependencies; the source size is persisted
            # automatically, for subsequent transfer rate calculations.
            job += DependentTask(task)
            tasks += 1

        log.info(f"Added {tasks} tasks to the job")

    log.info("Preparation phase complete")


def transfer(job_id:str) -> None:
    """ Transfer prepared tasks from Lustre to iRODS """
    _LOG_HEADER()

    state = _GET_STATE()
    state.register_filesystems(*_FILESYSTEMS)
    job = State.Job(state, client_id=_CLIENT, job_id=int(job_id))

    executor = _GET_EXECUTOR()
    worker = executor.worker

    log.info(f"Transfer phase: Worker {worker.id.worker}")

    # Launch follow-on worker, in case we run out of time
    # NOTE DAISYCHAIN can be set to abort accidental LSF proliferation
    following = job.metadata.DAISYCHAIN == "Yes"
    if following:
        follow_on, follow_options = _transfer_worker(job_id, T.Path(job.metadata.logs))
        follow_on.specific_worker = worker.id.worker
        follow_on += worker.id
        follow_runner, *_ = executor.submit(follow_on, follow_options)

        log.info(f"Follow-on worker submitted with LSF ID {follow_runner.job}; "
                 "will cancel on completion")

    # This is when we should wrap-up
    deadline = _START_TIME + worker.limit(LSFWorkerLimit.Runtime) - _FUDGE_TIME

    # Don't start the transfer phase until preparation has started
    while job.status.phase(_PREPARE).start is None:
        # Check we're not going to overrun the limit (which shouldn't
        # happen when just waiting for the preparation phase to start)
        if time.now() > deadline:
            log.info("Approaching runtime limit; terminating")
            sys.exit(0)

        log.info("Waiting for preparation phase to start...")
        sleep(_FUDGE_TIME.total_seconds())

    # Initialise the transfer phase (idempotent)
    job.status.phase(_TRANSFER).init()

    log.info("Starting transfers")

    while not job.status.complete:
        remaining_time = deadline - time.now()

        try:
            attempt = job.attempt(remaining_time)

        except NoTasksAvailable:
            # Check if we're done
            current = job.status

            if current.phase(_PREPARE) or current.pending > 0:
                # Preparation phase is still in progress, or there are
                # still pending tasks
                log.warning("Cannot complete any more tasks in the given run limit; terminating")

            else:
                # All tasks have been prepared and none are pending, so
                # cancel the follow-on
                log.info("Nothing left do to for this worker")

                if following:
                    log.info(f"Cancelling follow-on worker with LSF ID {follow_runner.job}")
                    executor.signal(follow_runner, SIGTERM)

                # If no tasks are in-flight, then we're finished
                if current.running == 0:
                    log.info(f"All tasks complete")
                    job.status.phase(_TRANSFER).stop()

            sys.exit(0)

        log.info("Attempting transfer of "
                 f"{attempt.task.source.address} on {attempt.task.source.filesystem} to "
                 f"{attempt.task.target.address} on {attempt.task.target.filesystem}")

        # TODO Py3.8 walrus operator would be good here
        success = attempt()
        if success:
            log.info(f"Successfully transferred and verified {_human_size(attempt.size(DataOrigin.Source))}B")


def status(job_id:str) -> None:
    """ Report job status to user """
    _LOG_HEADER()

    state = _GET_STATE()
    job = State.Job(state, client_id=_CLIENT, job_id=int(job_id))
    current = job.status

    if not current.phase(_PREPARE).complete:
        log.warning(f"Preparation phase for job {job_id} is still in progress, "
                    "the following output may be incomplete")

    log.info(f"Pending: {current.pending}")
    log.info(f"Running: {current.running}")
    log.info(f"Failed: {current.failed}")
    log.info(f"Succeeded: {current.succeeded}")

    try:
        # NOTE This is specific to Lustre-to-iRODS tasks
        throughput = current.throughput(*_FILESYSTEMS)
        log.info(f"Transfer rate: {_human_size(throughput.transfer_rate)}B/s")
        log.info(f"Failure rate: {throughput.failure_rate:.1%}")

    except NoThroughputData:
        log.info("Transfer rate: No data")
        log.info("Failure rate: No data")
