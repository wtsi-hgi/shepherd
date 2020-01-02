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

# We need postponed annotation evaluation for our recursive definitions
# https://docs.python.org/3/whatsnew/3.7.html#pep-563-postponed-evaluation-of-annotations
from __future__ import annotations

from pathlib import Path

from psycopg2.extensions import cursor

from common import types as T
from common.exceptions import NOT_IMPLEMENTED
from common.logging import log, Level
from common.models.filesystems.types import BaseFilesystem, Data
from common.models.task import ExitCode
from .db import PostgreSQL
from ..types import BasePhaseStatus, BaseJobStatus, BaseAttempt, BaseJob, \
                    JobPhase, JobThroughput, DependentTask, DataOrigin
from ..exceptions import *


_SCHEMA = Path("lib/state/postgresql/schema.sql")


# Map our Python JobPhase enum to our PostgreSQL job_phase enum
_PG_PHASE_ENUM = {
    JobPhase.Preparation: "prepare",
    JobPhase.Transfer:    "transfer"
}

class PGPhaseStatus(BasePhaseStatus):
    _state:PostgreSQL
    _job_id:T.Identifier
    _phase:str

    def __init__(self, state:PostgreSQL, job_id:T.Identifier, phase:JobPhase) -> None:
        self._state = state
        self._job_id = job_id
        self._phase = _PG_PHASE_ENUM[phase]

        self.init()

    def init(self) -> T.DateTime:
        # Set the start time, if it hasn't been already, and return it
        with self._state.transaction() as c:
            # NOTE In the below, the returning clause will only return
            # if a change was made, thus we forcibly make a redundant
            # change (rather than "do nothing") on conflicts
            c.execute("""
                insert into job_timestamps (job, phase)
                                    values (%s, %s)
                               on conflict (job, phase)
                             do update set phase = excluded.phase
                                 returning start;
            """, (self._job_id, self._phase))

            self.start = c.fetchone().start

        return self.start

    def stop(self) -> T.DateTime:
        # Set the finish time, if it hasn't been already, and return it;
        with self._state.transaction() as c:
            # NOTE The start time is recorded, if necessary, as part of
            # the constructor, so an update will always be possible
            c.execute("""
                update    job_timestamps
                set       finish = coalesce(finish, now())
                where     job    = %s
                and       phase  = %s
                returning finish;
            """, (self._job_id, self._phase))

            self.finish = c.fetchone().finish

        return self.finish


class PGJobStatus(BaseJobStatus):
    _state:PostgreSQL
    _job_id:T.Identifier

    def __init__(self, state:PostgreSQL, job_id:T.Identifier) -> None:
        self._state = state
        self._job_id = job_id

        with state.transaction() as c:
            c.execute("""
                select   sum(pending)   as pending,
                         sum(running)   as running,
                         sum(failed)    as failed,
                         sum(succeeded) as succeeded
                from     job_status
                where    job = %s
                group by job;
            """, (job_id,))

            status = c.fetchone()
            self.pending   = status.pending
            self.running   = status.running
            self.failed    = status.failed
            self.succeeded = status.succeeded

    def throughput(self, source:BaseFilesystem, target:BaseFilesystem) -> JobThroughput:
        with self._state.transaction() as c:
            c.execute("""
                select job_throughput.transfer_rate,
                       job_throughput.failure_rate
                from   job_throughput
                join   filesystems as source_fs
                on     source_fs.id = job_throughput.source
                join   filesystems as target_fs
                on     target_fs.id = job_throughput.target
                where  job_throughput.job = %s
                and    source_fs.name     = %s
                and    target_fs.name     = %s;
            """, (self._job_id, source.name, target.name))

            # TODO Py3.8 walrus operator would be good here
            rates = c.fetchone()
            if rates is None:
                raise NoThroughputData(f"Not enough data to calculate throughput rates for job {self._job_id}")

        return JobThroughput(rates.transfer_rate, rates.failure_rate)

    def phase(self, phase:JobPhase) -> PGPhaseStatus:
        return PGPhaseStatus(self._state, self._job_id, phase)


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

        except LogicException as e:
            message = f"Could not create schema\n{e}"
            raise LogicException(message)

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

    def _add_data(self, c:cursor, data:Data, persist_size:bool = False) -> T.Identifier:
        # Add data record (filesystem and address) to database

        # NOTE In the below, the returning clause will only return if a
        # change was made, thus we forcibly make a redundant change
        # (rather than "do nothing") on conflicts
        c.execute("""
            insert into filesystems (job, name, max_concurrency)
                             values (%s, %s, %s)
                        on conflict (job, name)
                      do update set name = excluded.name
                          returning id;
        """, (self._job_id, data.filesystem.name, data.filesystem.max_concurrency))

        filesystem_id = c.fetchone().id

        c.execute("""
            insert into data (filesystem, address)
                      values (%s, %s)
                   returning id;
        """, (filesystem_id, str(data.address)))

        data_id = c.fetchone().id

        if persist_size:
            # The root source data size should be persisted
            filesize = data.filesystem.size(data.address)
            c.execute("insert into size (data, size) values (%s, %s);", (data_id, filesize))

        return data_id

    @staticmethod
    def _get_target_id(c:cursor, task_id:T.Identifier) -> T.Identifier:
        # Get the target data identifier for a task
        c.execute("select target from tasks where id = %s;", (task_id,))
        return c.fetchone().target

    def _add_task_tree(self, task:T.Optional[DependentTask]) -> T.Optional[T.Identifier]:
        if task is None:
            return None

        # Recursively add dependencies until we bottom out (i.e., above)
        dependency = self._add_task_tree(task.dependency)
        root_task = dependency is None

        # Add task
        with self._state.transaction() as c:
            task_values = {
                "job_id":        self.job_id,

                # The source of a task is the same as the target of its
                # dependency, if it has one, so only add data records to
                # the database when we need to; otherwise we'd trip over
                # the uniqueness constraint set by the schema
                "source_id":     self._add_data(c, task.task.source, True) if root_task else \
                                 PGJob._get_target_id(c, dependency),
                "target_id":     self._add_data(c, task.task.target),

                "script":        task.task.script,
                "dependency_id": dependency
            }

            c.execute("""
                insert into tasks (job, source, target, script, dependency)
                           values (%(job_id)s, %(source_id)s, %(target_id)s, %(script)s, %(dependency_id)s)
                        returning id;
            """, task_values)

            return c.fetchone().id

    def __iadd__(self, task:DependentTask) -> PGJob:
        _ = self._add_task_tree(task)
        return self

    def attempt(self, time_limit:T.Optional[T.TimeDelta] = None) -> PGAttempt:
        raise NOT_IMPLEMENTED

    @property
    def max_attempts(self) -> int:
        with self._state.transaction() as c:
            c.execute("""
                select max_attempts from jobs where id = %s;
            """, (self.job_id,))

            return c.fetchone().max_attempts

    @max_attempts.setter
    def max_attempts(self, value:int) -> None:
        with self._state.transaction() as c:
            c.execute("""
                update jobs
                set    max_attempts = %s
                where  id           = %s;
            """, (value, self.job_id))

    @property
    def status(self) -> PGJobStatus:
        return PGJobStatus(self._state, self.job_id)

    @property
    def metadata(self) -> T.SimpleNamespace:
        with self._state.transaction() as c:
            c.execute("""
                select key, value from job_metadata where job = %s;
            """, (self.job_id,))

            return T.SimpleNamespace(**{k:v for k, v in c.fetchall() or {}})

    def set_metadata(self, **metadata:str) -> None:
        with self._state.transaction() as c:
            for k, v in metadata.items():
                c.execute("""
                    insert into job_metadata (job, key, value)
                                      values (%s, %s, %s)
                                 on conflict (job, key)
                               do update set value = excluded.value;
                """, (self.job_id, k, v))
