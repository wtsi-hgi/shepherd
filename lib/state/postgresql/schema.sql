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

-- Task status: An annotated view of attempts, which includes tasks that
-- have yet to be attempted; the latter of which have an attempt index
-- of 0 and are semantically "unsuccessful".
create view if not exists task_status as
  select attempts.task,                                                    -- Task ID
         row_number() over history as attempt,                             -- Attempt index
         attempts.start,                                                   -- Start timestamp
         attempts.finish,                                                  -- Finish timestamp
         attempts.exit_code                                                -- Exit code
         (row_number() over history) = (count(1) over history) as latest,  -- Latest predicate
         attempts.exit_code = 0 as succeeded                               -- Success predicate (null => in progress)
  from   attempts
  window history as (partition by attempts.task
                     order by     attempts.start asc)

  union all

  -- Unattempted tasks
  select    tasks.id as task,
            0        as attempt,
            null     as start,
            null     as finish,
            null     as exit_code,
            true     as latest,
            false    as succeeded
  from      tasks
  left join attempts
  on        attempts.task = tasks.id
  where     attempts.task is null;

commit;




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
