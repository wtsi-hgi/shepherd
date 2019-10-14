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

from common import types as T
from common.models.filesystems import POSIXFilesystem, iRODSFilesystem
from lib.planning.transformers import strip_common_prefix, last_n_components, prefix, telemetry, debugging
from lib.planning.route_factories import posix_to_irods_factory
from lib.state.types import JobStatus, DataNotReady
from lib.state.native import NativeJob, create_root


_FS = {
    "Lustre": POSIXFilesystem(name="Lustre", max_concurrency=50),
    "iRODS":  iRODSFilesystem(name="iRODS")
}


def print_status(status:JobStatus) -> None:
    print(f"* Pending:   {status.pending}")
    print(f"* Running:   {status.running}")
    print(f"* Failed:    {status.failed}")
    print(f"* Succeeded: {status.succeeded}")


def create_state_from_fofn(fofn:T.Path) -> None:
    """ Lustre to iRODS state from FoFN """
    transfer = posix_to_irods_factory(_FS["Lustre"], _FS["iRODS"])
    transfer += strip_common_prefix
    transfer += prefix(T.Path("/here/is/a/prefix/for/humgen/shepherd_testing"))
    transfer += last_n_components(4)
    transfer += debugging
    transfer += telemetry

    state_root = create_root(T.Path("."))
    job = NativeJob(state_root)
    job.filesystem_mapping = _FS

    # FIXME the maximum concurrency is a property of the filesystems
    # involved in a task, rather than that of an entire job...
    job.max_concurrency = min(fs.max_concurrency for fs in _FS.values())

    print(f"State:            {state_root}")
    print(f"Job ID:           {job.job_id}")
    print(f"Max. Attempts:    {job.max_attempts}")
    print(f"Max. Concurrency: {job.max_concurrency}")

    tasks = 0
    files = _FS["Lustre"]._identify_by_fofn(fofn)
    for task in transfer.plan(files):
        print(("=" if tasks == 0 else "-") * 72)

        job += task
        tasks += 1

        print(f"Source: {task.source.filesystem} {task.source.address}")
        print(f"Target: {task.target.filesystem} {task.target.address}")

    print("=" * 72)
    print(f"Added {tasks} tasks to job:")
    print_status(job.status)


def run_state(state_root:str, job_id:int, worker_index:T.Optional[int] = None) -> None:
    """ Run through tasks in state database """
    job = NativeJob(T.Path(state_root), job_id=job_id, force_restart=True)
    job.filesystem_mapping = _FS

    print(f"State:            {state_root}")
    print(f"Job ID:           {job.job_id}")
    print(f"Max. Attempts:    {job.max_attempts}")
    print(f"Max. Concurrency: {job.max_concurrency}")

    if worker_index is not None:
        worker_index = int(worker_index)
        assert 0 <= worker_index < job.max_concurrency, "Worker index out of bounds"
        job.worker_index = worker_index
        print(f"Worker ID:        {worker_index}")

    print_status(job.status)

    tasks = 0
    while job.status:
        try:
            task = next(job)

        except StopIteration:
            break

        except DataNotReady:
            print("Not ready; sleeping...")
            sleep(60)
            continue

        print(("=" if tasks == 0 else "-") * 72)
        print(f"Source: {task.source.filesystem} {task.source.address}")
        print(f"Target: {task.target.filesystem} {task.target.address}")

        # TODO Check exit status and update state
        task()

        tasks += 1

    print("=" * 72)
    print("Done")
    print_status(job.status)


def main(*args:str) -> None:
    mode, *mode_args = args

    delegate = {
        "fofn": create_state_from_fofn,
        "exec": run_state
    }

    if mode not in delegate:
        print(f"No such mode {mode}!")
        exit(1)

    delegate[mode](*mode_args)
