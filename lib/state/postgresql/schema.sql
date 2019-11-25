/*
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
*/

begin transaction;

-- Schema versioning
do $$ declare
  schema date := timestamp '2019-11-22';
  actual date;
begin
  create table if not exists __version__ (version date primary key);
  select version into actual from __version__;

  if not found then
    insert into __version__ values (schema);
    actual := schema;
  end if;

  if actual != schema then
    raise exception 'Schema mismatch! Expected %; got %', schema, actual;
  end if;
end $$;


-- Jobs
create table if not exists jobs (
  id
    serial
    primary key,

  -- Maximum number of attempts per transfer
  max_attempts
    integer
    not null
    check (max_attempts > 0),

  -- Job start timestamp
  start
    timestamp with time zone
    not null
    default now(),

  -- Job finish timestamp
  finish
    timestamp with time zone
    check (finish is null or finish >= start)
);


-- Filesystems
create table if not exists filesystems (
  id
    serial
    primary key,

  -- Filesystem name
  name
    text
    not null
);


-- Data and its properties
create table if not exists data (
  id
    serial
    primary key,

  -- Filesystem
  filesystem
    integer
    not null
    references filesystems(id),

  -- Address
  address
    text
    not null,

  unique (filesystem, address)
);

create table if not exists size (
  data
    integer
    references data(id)
    primary key,

  -- Size in bytes
  size
    integer
    not null
    check (size >= 0)
);

create table if not exists checksum (
  data
    integer
    not null
    references data(id),

  algorithm
    text
    not null,

  checksum
    text
    not null,

  primary key (data, algorithm)
);

create table if not exists metadata (
  data
    integer
    not null
    references data(id),

  -- Key/value pair
  key
    text
    not null,

  value
    text
    not null,

  primary key (data, key)
);


-- Tasks and attempts
create table if not exists tasks (
  id
    serial
    primary key,

  job
    integer
    not null
    references jobs(id),

  -- Source data
  source
    integer
    not null
    references data(id),

  -- Target data
  target
    integer
    not null
    references data(id)
    check (source != target),

  -- Transfer script
  script
    text
    not null,

  -- Task dependency
  -- NOTE This only checks against circular references to itself; while
  -- we *can* check for cycles in SQL, we don't for performance and
  -- clarity reasons. It is up to the client to enforce a strict tree.
  dependency
    integer
    null
    references tasks(id)
    check (dependency is null or dependency != id),

  -- Every target and every source-target pair for a job must be unique;
  -- i.e., we can't transfer to the same target more than once and each
  -- source will only be transferred to its target at most once
  unique (job, target),
  unique (job, source, target)
);

create table if not exists attempts (
  task
    integer
    not null
    references tasks(id),

  -- Attempt start time
  start
    timestamp with time zone
    not null
    default now(),

  -- Attempt finish time (null for inflight)
  finish
    timestamp with time zone
    default null
    check (finish is null or finish >= start),

  -- Task's script's exit code (null for inflight)
  exit_code
    integer
    default null,

  primary key (task, start)
);

create index if not exists attempts_task      on attempts(task);
create index if not exists attempts_succeeded on attempts(task, attempt, exit_code) where exit_code = 0;
create index if not exists attempts_failed    on attempts(task, attempt, exit_code) where exit_code != 0;
create index if not exists attempts_running   on attempts(task, attempt, exit_code) where exit_code is null;

commit;





-- create table if not exists jobs (
--   id               integer  primary key,
--   start            integer  not null default (strftime('%s', 'now')),
--   finish           integer  check (finish is null or finish >= start)
-- );
-- 
-- create index if not exists jobs_id on jobs(id);
-- 
-- create table if not exists job_parameters (
--   job              integer  primary key references jobs(id),
--   max_attempts     integer  not null check (max_attempts > 0),
--   max_concurrency  integer  not null check (max_concurrency > 0)
-- ) without rowid;
-- 
-- create index if not exists job_params_job on job_parameters(job);
-- 
-- create table if not exists data (
--   id          integer  primary key,
--   filesystem  text     not null,
--   address     text     not null
-- );
-- 
-- create index if not exists data_id on data(id);
-- create index if not exists data_location on data(filesystem, address);
-- 
-- create table if not exists checksums (
--   data       integer  references data(id),
--   algorithm  text     not null,
--   checksum   text     not null,
-- 
--   primary key (data, algorithm)
-- ) without rowid;
-- 
-- create index if not exists checksum_data on checksums(data);
-- create index if not exists checksum_checksum on checksums(data, algorithm);
-- 
-- create table if not exists sizes (
--   data  integer  primary key references data(id),
--   size  integer  not null
-- ) without rowid;
-- 
-- create index if not exists sizes_data on sizes(data);
-- 
-- create table if not exists metadata (
--   data   integer  references data(id),
--   key    text     not null,
--   value  text     not null,
-- 
--   primary key (data, key)
-- ) without rowid;
-- 
-- create index if not exists metadata_data on metadata(data);
-- create index if not exists metadata_metadata on metadata(data, key);
-- 
-- create table if not exists tasks (
--   id          integer  primary key,
--   job         integer  not null references jobs(id),
--   source      integer  not null references data(id),
--   target      integer  not null references data(id) check (source != target),
--   script      text     not null,
--   dependency  integer  null references tasks(id) check (dependency is null or dependency != id),
-- 
--   unique (job, target),
--   unique (job, source, target)
-- );
-- 
-- create index if not exists tasks_id on tasks(id);
-- create index if not exists tasks_job on tasks(job);
-- 
-- create table if not exists attempts (
--   task       integer  references tasks(id),
-- 
--   -- NOTE The attempt ID must be an ascending integer, starting from 1
--   attempt    integer  not null,
-- 
--   start      integer  not null default (strftime('%s', 'now')),
--   finish     integer  check (finish is null or finish >= start),
--   exit_code  integer  default (null),
-- 
--   primary key (task, attempt)
-- );
-- 
-- create index if not exists attempts_task on attempts(task);
-- create index if not exists attempts_attempt on attempts(task, attempt);
-- create index if not exists attempts_succeeded on attempts(task, attempt, exit_code) where exit_code = 0;
-- create index if not exists attempts_failed on attempts(task, attempt, exit_code) where exit_code is not null and exit_code != 0;
-- create index if not exists attempts_running on attempts(task, attempt, exit_code) where exit_code is null;

create view if not exists task_status as
  select    latest.task,
            latest.attempt,
            latest.exit_code
  from      attempts as latest
  left join attempts as later
  on        later.task    = latest.task
  and       later.attempt > latest.attempt
  where     later.task   is null

  union all

  -- NOTE Unattempted tasks are denoted as "failed"
  select    tasks.id as task,
            0        as attempt,
            1        as exit_code
  from      tasks
  left join attempts
  on        attempts.task  = tasks.id
  where     attempts.task is null;

-- Overall job status, partitioned by worker
create view if not exists job_status as
  select   tasks.job,
           tasks.id % job_parameters.max_concurrency as worker_id,

           -- Pending: Non-zero exit code and fewer attempts than maximum
           sum(case when
             task_status.attempt < job_parameters.max_attempts
               and task_status.exit_code is not null
               and task_status.exit_code != 0
             then 1
             else 0
           end) as pending,

           -- Running: Null exit code
           sum(case when
             task_status.exit_code is null
             then 1
             else 0
           end) as running,

           -- Failed: Non-zero exit code at maximum attempts
           sum(case when
             task_status.attempt = job_parameters.max_attempts
               and task_status.exit_code is not null
               and task_status.exit_code != 0
             then 1
             else 0
           end) as failed,

           -- Succeeded: Zero exit code
           sum(case when
             task_status.exit_code = 0
             then 1
             else 0
           end) as succeeded,

           max(jobs.start)  as start,
           max(jobs.finish) as finish

  from     task_status
  join     tasks
  on       tasks.id = task_status.task
  join     jobs
  on       jobs.id = tasks.job
  join     job_parameters
  on       job_parameters.job = jobs.id
  group by tasks.job,
           worker_id;

-- Tasks ready to be actioned
create view if not exists todo as
  select    tasks.id as task,
            tasks.job,
            source.filesystem as source_filesystem,
            source.address    as source_address,
            target.filesystem as target_filesystem,
            target.address    as target_address,
            tasks.script
  from      task_status
  join      tasks
  on        tasks.id = task_status.task
  left join task_status as dependency
  on        dependency.task = tasks.dependency
  join      data as source
  on        source.id = tasks.source
  join      data as target
  on        target.id = tasks.target
  join      jobs
  on        jobs.id = tasks.job
  join      job_parameters
  on        job_parameters.job = jobs.id
  where     jobs.finish is null
  and       task_status.exit_code is not null
  and       task_status.exit_code != 0
  and       (tasks.dependency is null or dependency.exit_code = 0)
  and       task_status.attempt < job_parameters.max_attempts;

commit;
