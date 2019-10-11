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

from common import types as T
from common.models.filesystems import POSIXFilesystem, iRODSFilesystem
from lib.planning.transformers import strip_common_prefix, last_n_components, prefix, telemetry, debugging
from lib.planning.route_factories import posix_to_irods_factory
from lib.state.native import NativeJob, create_root

def main(*args:str) -> None:
    fofn, *_ = args

    lustre = POSIXFilesystem(name="Lustre", max_concurrency=50)
    irods = iRODSFilesystem()

    transfer = posix_to_irods_factory(lustre, irods)
    transfer += strip_common_prefix
    transfer += prefix(T.Path("/here/is/a/prefix"))
    transfer += last_n_components(4)
    transfer += debugging
    transfer += telemetry

    state_root = create_root(T.Path("."))
    state = NativeJob(state_root)

    print(f"State:  {state_root}")
    print(f"Job ID: {state.job_id}")

    tasks = 0
    files = lustre._identify_by_fofn(T.Path(fofn))
    for task in transfer.plan(files):
        print(("=" if tasks == 0 else "-") * 72)

        state += task
        tasks += 1

        print(f"Source: {task.source.filesystem} {task.source.address}")
        print(f"Target: {task.target.filesystem} {task.target.address}")

    print("=" * 72)
    print(f"Added {tasks} tasks to job:")

    status = state.status
    print(f"* Pending:   {status.pending}")
    print(f"* Running:   {status.running}")
    print(f"* Failed:    {status.failed}")
    print(f"* Succeeded: {status.succeeded}")
