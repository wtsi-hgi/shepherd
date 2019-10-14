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

import stat
import sqlite3
from tempfile import mkdtemp

from common import types as T
from common.models.task import Task
from common.models.filesystems.types import Data
from ..types import BaseJob, DataNotReady, JobStatus, WorkerRedundant


# Internal constants
_TEMP_PREFIX = "shepherd-state_"
_STATE_DB    = "state.db"
_SCHEMA      = T.Path("lib/state/native/schema.sql")
_DB_TIMEOUT  = 3600  # 1hr


def create_root(parent:T.Optional[T.Path] = None):
    """
    Create a temporary root directory to contain the state

    @param   parent  Parent directory (default None, i.e., use the
                     system temporary directory)
    @return  Root directory path
    """
    if parent is not None:
        if not parent.is_dir():
            raise FileNotFoundError(f"Cannot create state in {parent}; no such directory")

        parent = parent.absolute()

    return T.Path(mkdtemp(prefix=_TEMP_PREFIX, dir=parent))


_JOB_STATUS_ALL = r"""
    select   sum(pending),
             sum(running),
             sum(failed),
             sum(succeeded),
             max(start),
             max(finish)
    from     job_status
    where    job = ?
    group by job
"""

_NEXT_TASK_ALL = r"""
    select task,
           source_filesystem,
           source_address,
           target_filesystem,
           target_address,
           script
    from   todo
    where  job = ?
"""

_JOB_STATUS_BY_WORKER = r"""
    select pending,
           running,
           failed,
           succeeded,
           start,
           finish
    from   job_status
    where  job       = ?
    and    worker_id = ?
"""

_NEXT_TASK_BY_WORKER = r"""
    select todo.task,
           todo.source_filesystem,
           todo.source_address,
           todo.target_filesystem,
           todo.target_address,
           todo.script
    from   todo
    join   job_parameters
    on     job_parameters.job = todo.job
    where  todo.job                                   = ?
    and    todo.task % job_parameters.max_concurrency = ?
"""


class NativeJob(BaseJob):
    """ SQLite-Based Persistence Engine """
    _worker_index:T.Optional[int]

    def __init__(self, state:T.Path, *, job_id:T.Optional[int] = None, force_restart:bool = False) -> None:
        # Create state root, if it doesn't exist
        state.mkdir(mode=stat.S_IRWXU, parents=True, exist_ok=True)
        self._state = state

        # Connect to state database
        db = state / _STATE_DB
        self._db = sqlite3.connect(db, timeout=_DB_TIMEOUT)

        # Create schema
        with self._db as conn:
            schema = _SCHEMA.read_text()
            conn.executescript(schema)

        if job_id is not None and force_restart:
            # Reset running task status on resumption
            with self._db as conn:
                conn.execute("""
                    with previously_running as (
                        select task,
                               attempt
                        from   task_status
                        where  exit_code is null
                    )
                    update attempts
                    set    finish    = strftime('%s', 'now'),
                           exit_code = 1
                    where  (task, attempt) in (select task, attempt from previously_running)
                """)

        # FIXME We hardcode the worker index, max_attempts and
        # max_concurrency for now... This needs more thought!
        self._worker_index = None
        max_attempts       = 3
        max_concurrency    = 1

        if job_id is None:
            # Create new job
            with self._db as conn:
                cur = conn.execute("insert into jobs default values")
                job_id = cur.lastrowid

                conn.execute(
                    "insert into job_parameters(job, max_attempts, max_concurrency) values (?, ?, ?)",
                    (job_id, max_attempts, max_concurrency))

        self._job_id = job_id

    def __iadd__(self, task:Task) -> NativeJob:
        with self._db as conn:
            # Insert new data objects
            data = { "source": task.source, "target": task.target }
            data_id = {}

            for data_name, data_obj in data.items():
                cur = conn.execute(
                    "insert into data(filesystem, address) values (?, ?)",
                    (data_obj.filesystem.name, str(data_obj.address)))
                data_id[data_name] = cur.lastrowid

            # TODO Task dependencies
            conn.execute(
                "insert into tasks(job, source, target, script, dependency) values (?, ?, ?, ?, ?)",
                (self.job_id, data_id["source"], data_id["target"], task.script, None))

        return self

    def __next__(self) -> Task:
        if self.status.pending == 0:
            raise StopIteration("No more pending tasks for worker")

        with self._db as conn:
            sql_params = (_NEXT_TASK_ALL, (self.job_id,)) if self._worker_index is None \
                    else (_NEXT_TASK_BY_WORKER, (self.job_id, self.worker_index))

            cur = conn.execute(*sql_params)
            task_id, *task_details = cur.fetchone() or (None, None)

            if task_id is not None:
                # Create new attempt
                cur = conn.execute(
                    "select attempt from task_status where task = ?",
                    (task_id,))

                last_attempt, *_ = cur.fetchone()

                conn.execute(
                    "insert into attempts(task, attempt) values (?, ?)",
                    (task_id, last_attempt + 1))

                # Create Task object from database record
                names      = ["source", "target"]
                filesystem = {}
                address    = {}

                filesystem["source"], address["source"], \
                    filesystem["target"], address["target"], \
                    script = task_details

                data = {
                    data_name: Data(
                        filesystem = self.filesystem_mapping[filesystem[data_name]],
                        address    = T.Path(address[data_name]))
                    for data_name in names}

                return Task(script=script, **data)

        raise DataNotReady("Pending tasks have unresolved dependencies")

    @property
    def status(self) -> JobStatus:
        with self._db as conn:
            sql_params = (_JOB_STATUS_ALL, (self.job_id,)) if self._worker_index is None \
                    else (_JOB_STATUS_BY_WORKER, (self.job_id, self.worker_index))

            cur = conn.execute(*sql_params)
            status = cur.fetchone()  # TODO Convert start (and finish) to DateTime

            if status is None:
                raise WorkerRedundant(f"Worker {self.worker_index} has nothing to do")

        return JobStatus(*status)

    # FIXME? All this injection stuff is a bit primitive...
    @property
    def worker_index(self) -> T.Optional[int]:
        return self._worker_index

    @worker_index.setter
    def worker_index(self, value:int) -> None:
        self._worker_index = value

    def _job_params(self) -> T.Tuple[int, int]:
        with self._db as conn:
            max_attempts, max_concurrency = conn.execute(
                "select max_attempts, max_concurrency from job_parameters where job = ?",
                (self.job_id,)).fetchone()

        return max_attempts, max_concurrency

    @property
    def max_attempts(self) -> int:
        max_attempts, _ = self._job_params()
        return max_attempts

    @max_attempts.setter
    def max_attempts(self, value:int) -> None:
        assert value > 0

        with self._db as conn:
            conn.execute(
                "update job_parameters set max_attempts = ? where job = ?",
                (value, self.job_id))

    @property
    def max_concurrency(self) -> int:
        _, max_concurrency = self._job_params()
        return max_concurrency

    @max_concurrency.setter
    def max_concurrency(self, value:int) -> None:
        # TODO Abstract this (see max_attempts.setter)
        assert value > 0

        with self._db as conn:
            conn.execute(
                "update job_parameters set max_concurrency = ? where job = ?",
                (value, self.job_id))
