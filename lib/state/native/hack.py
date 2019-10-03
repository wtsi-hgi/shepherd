# DO NOT USE THIS POS IN PRODUCTION!!!
# This is a total hack job to prove that the underlying database works
# under the current time pressures. Replace with something good!

import sqlite3
from pathlib import Path
from tempfile import mkdtemp


# Internal constants
_TEMP_PREFIX = "shepherd-state_"
_STATE_DB    = "state.db"
_DB_TIMEOUT  = 3600 # 1hr


def create_root(parent = None):
    if parent is not None:
        if not parent.is_dir():
            raise FileNotFoundError(f"Cannot create state in {parent}; no such directory")

        parent = parent.absolute()

    return Path(mkdtemp(prefix=_TEMP_PREFIX, dir=parent))


class HackityHackHack:
    def __init__(self, root, job = None, max_attempts = 3):
        root.mkdir(mode=0o700, parents=True, exist_ok=True)
        self._root = root
        db = root / _STATE_DB
        self._db = sqlite3.connect(db, timeout=_DB_TIMEOUT)

        # Create schema
        with self._db as conn:
            with open("lib/state/native/schema.sql", "rt") as f:
                schema = f.read()

            conn.executescript(schema)

        if job is None:
            # Create new job
            with self._db as conn:
                cur = conn.execute("insert into jobs default values")
                job = cur.lastrowid

                cur = conn.execute(
                    "insert into job_parameters(job, max_attempts, max_concurrency) values (?, ?, ?)",
                    (job, max_attempts, 1))

        self._job = job

    @property
    def job(self):
        return self._job

    @property
    def max_concurrency(self):
        with self._db as conn:
            max_concurrency, = conn.execute(
                "select max_concurrency from job_parameters where job = ?",
                (self.job,)).fetchone()

        return max_concurrency

    @max_concurrency.setter
    def max_concurrency(self, value):
        assert value > 0

        with self._db as conn:
            conn.execute(
                "update job_parameters set max_concurrency = ? where job = ?",
                (value, self.job))

    def add_data(self, filesystem, address):
        with self._db as conn:
            cur = conn.execute(
                "insert into data(filesystem, address) values (?, ?)",
                (filesystem, address))
            data_id = cur.lastrowid

        return data_id

    def add_task(self, script, source, source_fs, target, target_fs, dependency = None):
        # FIXME Tight coupling
        source_id = self.add_data(source_fs, source)
        target_id = self.add_data(target_fs, target)

        with self._db as conn:
            cur = conn.execute(
                "insert into tasks(job, source, target, script, dependency) values (?, ?, ?, ?, ?)",
                (self._job, source_id, target_id, script, dependency))
            task_id = cur.lastrowid

        return task_id
