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

from common import types as T
from common.exceptions import NOT_IMPLEMENTED
from common.models.task import ExitCode, Task
from common.models.filesystems.types import BaseFilesystem
# from .db import SOMETHING
from ..types import BasePhaseStatus, BaseJobStatus, BaseAttempt, BaseJob, \
                    JobPhase, JobThroughput, DependentTask, DataOrigin
from ..exceptions import *


class PGPhaseStatus(BasePhaseStatus):
    def init(self) -> T.DateTime:
        raise NOT_IMPLEMENTED

    def stop(self) -> T.DateTime:
        raise NOT_IMPLEMENTED


class PGJobStatus(BaseJobStatus):
    def throughput(self, source:BaseFilesystem, target:BaseFilesystem) -> JobThroughput:
        raise NOT_IMPLEMENTED

    def phase(self, phase:JobPhase) -> PGPhaseStatus:
        raise NOT_IMPLEMENTED


class PGAttempt(BaseAttempt):
    def init(self) -> T.DateTime:
        raise NOT_IMPLEMENTED

    def stop(self) -> T.DateTime:
        raise NOT_IMPLEMENTED

    def size(self, origin:DataOrigin) -> int:
        raise NOT_IMPLEMENTED

    def checksum(self, origin:DataOrigin, algorithm:str) -> str:
        raise NOT_IMPLEMENTED

    @property
    def exit_code(self) -> ExitCode:
        raise NOT_IMPLEMENTED

    @exit_code.setter
    def exit_code(self, value:ExitCode) -> None:
        raise NOT_IMPLEMENTED


class PGJob(BaseJob):
    def __init__(self, state:T.Any, *, client_id:str, job_id:T.Optional[T.Identifier] = None, force_restart:bool = False) -> None:
        raise NOT_IMPLEMENTED

    def __iadd__(self, task:DependentTask) -> PGJob:
        raise NOT_IMPLEMENTED

    def attempt(self, time_limit:T.Optional[T.TimeDelta] = None) -> PGAttempt:
        raise NOT_IMPLEMENTED

    @property
    def max_attempts(self) -> int:
        raise NOT_IMPLEMENTED

    @max_attempts.setter
    def max_attempts(self, value:int) -> None:
        raise NOT_IMPLEMENTED

    @property
    def status(self) -> PGJobStatus:
        raise NOT_IMPLEMENTED

    @property
    def client_metadata(self) -> T.Dict[str, str]:
        raise NOT_IMPLEMENTED

    def set_client_metadata(self, **metadata:str) -> None:
        raise NOT_IMPLEMENTED
