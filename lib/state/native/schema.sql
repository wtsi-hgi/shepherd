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

pragma foreign_keys = ON;

begin exclusive transaction;

create table if not exists jobs (
  id               integer  primary key,
  start            integer  not null default (strftime('%s', 'now')),
  finish           integer  check (finish is null or finish >= start),
  max_attempts     integer  not null check (max_attempts > 0),
  max_concurrency  integer  not null check (max_concurrency > 0)
);

create index if not exists jobs_pk on jobs(id);

create table if not exists data (
  id          integer  primary key,
  filesystem  text     not null,
  location    text     not null
);

create index if not exists data_pk on data(id);
create index if not exists data_location on data(filesystem, location);

create table if not exists checksums (
  data       integer  primary key references data(id),
  algorithm  text     not null,
  checksum   text     not null
) without rowid;

create index if not exists checksum_data on checksums(data);

create table if not exists sizes (
  data  integer  primary key references data(id),
  size  integer  not null
) without rowid;

create index if not exists sizes_data on sizes(data);

create table if not exists metadata (
  data   integer  primary key references data(id),
  key    text     not null,
  value  text     not null,

  unique (data, key)
) without rowid;

create index if not exists metadata_data on metadata(data);
create index if not exists metadata_key on metadata(data, key);

create table if not exists tasks (
  id          integer  primary key,
  job         integer  references jobs(id),
  source      integer  references data(id),
  target      integer  references data(id) check (source != target),
  script      text     not null,
  dependency  integer  null references tasks(id) check (dependency is null or dependency != id),

  unique (job, target),
  unique (job, source, target)
);

create index if not exists tasks_pk on tasks(id);
create index if not exists tasks_job on tasks(job);

create table if not exists attempts (
  task       integer  references tasks(id),
  attempt    integer  not null,
  start      integer  not null default (strftime('%s', 'now')),
  finish     integer  check (finish is null or finish >= start),
  exit_code  integer  default (null),

  primary key (task, attempt),
  unique (task, attempt)
);

create index if not exists attempts_task on attempts(task);
create index if not exists attempts_attempt on attempts(task, attempt);
create index if not exists attempts_succeeded on attempts(task, attempt, exit_code) where exit_code = 0;
create index if not exists attempts_failed on attempts(task, attempt, exit_code) where exit_code is not null and exit_code != 0;

-- TODO Use recursive CTEs?
create view if not exists to_transfer as
  with failed as (
    select  attempts.task,
            attempts.attempt
    from    attempts
    where   attempts.exit_code is not null
    and     attempts.exit_code != 0
  ),
  fail_count as (
    select   failed.task,
             count(failed.attempt) as num_failed
    from     failed
    group by failed.task
  ),
  succeeded as (
    select  attempts.task,
            attempts.attempt
    from    attempts
    where   attempts.exit_code = 0
  ),
  todo as (
    select    failed.task as task,
              fail_count.num_failed
    from      failed
    join      fail_count
    on        fail_count.task = failed.task
    left join succeeded
    on        succeeded.task    = failed.task
    and       succeeded.attempt > failed.attempt
    where     succeeded.task is null

    union all

    select    tasks.id as task,
              0 as num_failed
    from      tasks
    left join attempts
    on        attempts.task = tasks.id
    where     attempts.task is null
  )
  select    ready.task,
            tasks.job,
            source.filesystem as source_filesystem,
            source.location   as source_location,
            target.filesystem as target_filesystem,
            target.location   as target_location,
            tasks.script
  from      todo as ready
  join      tasks
  on        tasks.id = ready.task
  left join todo as pending
  on        pending.task = tasks.dependency
  join      jobs
  on        jobs.id = tasks.job
  join      data as source
  on        source.id = tasks.source
  join      data as target
  on        target.id = tasks.target
  where     pending.task is null
  and       ready.num_failed < jobs.max_attempts;

commit;
