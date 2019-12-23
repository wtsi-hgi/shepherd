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

from pathlib import Path

from psycopg2.errors import RaiseException

from common import types as T
from common.exceptions import NOT_IMPLEMENTED
from common.logging import log, Level
from common.models.filesystems.types import BaseFilesystem
from common.models.task import ExitCode, Task
from .db import PostgreSQL
from ..types import BasePhaseStatus, BaseJobStatus, BaseAttempt, BaseJob, \
                    JobPhase, JobThroughput, DependentTask, DataOrigin
from ..exceptions import *


_SCHEMA = Path("lib/state/postgresql/schema.sql")


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
    _state:PostgreSQL

    def __init__(self, state:PostgreSQL, *, client_id:str, job_id:T.Optional[T.Identifier] = None, force_restart:bool = False) -> None:
        self._state = state
        self._client_id = client_id

        # Create schema (idempotent)
        try:
            state.execute_script(_SCHEMA)

        except RaiseException as e:
            raise BackendException(f"Could not create schema\n{e.pgerror}")

        # Check previous job exists under the same client
        if job_id is not None:
            with state.transaction() as c:
                c.execute("""
                    select * from jobs where id = %s and client = %s;
                """, (job_id, client_id))

                if c.fetchone() is None:
                    raise BackendException(f"Job {job_id} does not exist or was started with a different client")

        # Reset previously running task status on resumption
        if job_id is not None and force_restart:
            if any(self.status.phase(phase) for phase in JobPhase):
                raise DataNotReady(f"Cannot restart job {job_id}; still in progress")

            with state.transaction() as c:
                c.execute("""
                    with previously_running as (
                        select task,
                               start
                        from   task_status
                        where  succeeded is null
                    )
                    update attempts
                    set    finish    = now(),
                           exit_code = 1
                    where  (task, start) in (select task, start from previously_running);
                """)

        # Create new job
        if job_id is None:
            with state.transaction() as c:
                c.execute("""
                    insert into jobs (client, max_attempts)
                              values (%s, 1)
                           returning id;
                """, (client_id,))

                job_id = c.fetchone().id

        self._job_id = job_id

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
