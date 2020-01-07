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

import subprocess
import shlex

from common import types as T
from common.logging import log
from ..types import WorkerIdentifier


def lsf_job_id(identifier:WorkerIdentifier) -> str:
    """ Render a worker identifier as an LSF job ID """
    job_id = str(identifier.job)

    if identifier.worker is not None:
        job_id += f"[{identifier.worker}]"

    return job_id


def run(command:str, *, env:T.Optional[T.Dict[str, str]] = None) -> subprocess.CompletedProcess:
    """ Wrapper for running commands """
    log.debug(f"Running: {command}")
    return subprocess.run(
        shlex.split(command), env=env,
        capture_output=True, check=False, text=True)
