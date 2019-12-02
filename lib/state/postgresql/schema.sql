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
  schema date := timestamp '2019-12-02';
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

  -- Client reference
  client
    text
    not null,

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

-- Job metadata
create table if not exists job_metadata (
  job
    integer
    not null
    references jobs(id),

  -- Key/value pair
  key
    text
    not null,

  value
    text
    not null,

  primary key (job, key)
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
create index if not exists attempts_succeeded on attempts(task, exit_code) where exit_code = 0;
create index if not exists attempts_failed    on attempts(task, exit_code) where exit_code != 0;
create index if not exists attempts_running   on attempts(task, exit_code) where exit_code is null;
create index if not exists attempts_completed on attempts(task, exit_code) where exit_code is not null;


-- Task status: An annotated view of attempts, which includes tasks that
-- have yet to be attempted; the latter of which have an attempt index
-- of 0 and are semantically "unsuccessful".
create or replace view task_status as
  select attempts.task,                                                    -- Task ID
         row_number() over history as attempt,                             -- Attempt index
         attempts.start,                                                   -- Start timestamp
         attempts.finish,                                                  -- Finish timestamp
         attempts.exit_code,                                               -- Exit code
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


-- Job throughput: A view of transfer and failure rates, based on the
-- source and target filesystem pairs used in a job. (NOTE We assume
-- that source-target pairs will be unique in any given job. This isn't
-- an unreasonable assumption, given the context of our goal, but
-- expectations ought to be managed.)
create or replace view job_throughput as
  select   jobs.id           as job,     -- Job ID
           source.filesystem as source,  -- Source filesystem
           target.filesystem as target,  -- Target filesystem

           -- Mean successful task completion rate (bytes/second)
           -- TODO Spread (e.g., standard deviation)?
           avg(case
             when task_status.succeeded then
               source_size.size / extract(epoch from task_status.finish - task_status.start)
             else null
           end) as transfer_rate,

           -- Mean attempt failure rate
           -- TODO Spread (e.g., standard deviation)?
           avg(case
             when task_status.succeeded then 0
             else 1
           end) as failure_rate

  from     task_status
  join     tasks
  on       tasks.id = task_status.task
  join     jobs
  on       jobs.id = tasks.job

  join     data as source
  on       source.id = tasks.source
  join     size as source_size
  on       source_size.data = source.id

  join     data as target
  on       target.id = tasks.target

           -- Completed tasks only
           -- NOTE "succeeded is not null" won't work, because our zero
           -- indexed (i.e., unattempted) tasks are marked as failed;
           -- this condition should use the attempts_completed index, so
           -- ought to be faster than alternatives.
  where    task_status.exit_code is not null

  group by jobs.id,
           source.filesystem,
           target.filesystem;


-- Job status: A view of counts of task states per job.
create or replace view job_status as
  select   tasks.job,

           -- Pending: Failed tasks with fewer attempts than maximum
           sum(case
             when task_status.attempt < jobs.max_attempts
               and not task_status.succeeded
               then 1
             else 0
           end) as pending,

           -- Running: Null success (i.e., no exit code)
           sum(case
             when task_status.succeeded is null then 1
             else 0
           end) as running,

           -- Failed: Non-zero exit code after maximum attempts
           -- (i.e., terminal failure)
           sum(case
             when task_status.attempt = jobs.max_attempts
               and not task_status.succeeded
               then 1
             else 0
           end) as failed,

           -- Succeeded: Zero exit code
           sum(case
             when task_status.succeeded then 1
             else 0
           end) as succeeded

  from     task_status
  join     tasks
  on       tasks.id = task_status.task
  join     jobs
  on       jobs.id = tasks.job
  where    task_status.latest
  group by tasks.job;


-- Tasks to do: A view of pending tasks and their estimated completion
-- time: data size / transfer rate * (1 - failure rate)
create or replace view todo as
  select    jobs.id  as job,
            tasks.id as task,

            -- Estimated time to completion (interval)
            make_interval(secs => source_size.size / (stats.transfer_rate * (1 - stats.failure_rate))) as eta

  from      task_status
  join      tasks
  on        tasks.id = task_status.task
  join      jobs
  on        jobs.id = tasks.job
  left join task_status as dependency
  on        dependency.task = tasks.dependency

  join      data as source
  on        source.id = tasks.source
  join      size as source_size
  on        source_size.data = source.id

  join      data as target
  on        target.id = tasks.target

  join      job_throughput as stats
  on        stats.job   = jobs.id
  and       stats.source = source.filesystem
  and       stats.target = target.filesystem

  where     jobs.finish is null
  and       task_status.latest
  and       task_status.attempt < jobs.max_attempts
  and       (tasks.dependency is null or dependency.succeeded)
  and       not task_status.succeeded;

commit;
