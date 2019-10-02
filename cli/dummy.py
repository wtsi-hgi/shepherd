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
from lib.planning.transfer import DataLocation
from lib.planning.filesystems import POSIXFilesystem, iRODSFilesystem
from lib.planning.transformers import strip_common_prefix, last_n_components, prefix, telemetry, debugging
from lib.planning.route_factories import posix_to_irods_factory
from lib.state.native.hack import HackityHackHack, create_root

def main(*args:str) -> None:
    fofn, *_ = args

    state_root = create_root(T.Path("."))
    print(f"State: {state_root}")
    state = HackityHackHack(state_root)
    print(f"Job ID: {state.job}")

    posix = POSIXFilesystem()
    irods = iRODSFilesystem()
    transfer = posix_to_irods_factory(posix, irods)
    transfer += strip_common_prefix
    transfer += prefix(DataLocation("/here/is/a/prefix"))
    transfer += last_n_components(3)
    transfer += debugging
    transfer += telemetry

    files = posix._identify_by_fofn(T.Path(fofn))
    for script, source, target in transfer.plan(files):
        task_id = state.add_task(source, target, script)
        print(f"Task {task_id}: {source} to {target}")
