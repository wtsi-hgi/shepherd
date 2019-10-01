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

create index if not exists jobs_id on jobs(id);

create table if not exists data (
  id          integer  primary key,
  filesystem  text     not null,
  address     text     not null
);

create index if not exists data_id on data(id);
create index if not exists data_location on data(filesystem, address);

create table if not exists checksums (
  data       integer  references data(id),
  algorithm  text     not null,
  checksum   text     not null,

  primary key (data, algorithm)
) without rowid;

create index if not exists checksum_data on checksums(data);
create index if not exists checksum_checksum on checksums(data, algorithm);

create table if not exists sizes (
  data  integer  primary key references data(id),
  size  integer  not null
) without rowid;

create index if not exists sizes_data on sizes(data);

create table if not exists metadata (
  data   integer  references data(id),
  key    text     not null,
  value  text     not null,

  primary key (data, key)
) without rowid;

create index if not exists metadata_data on metadata(data);
create index if not exists metadata_metadata on metadata(data, key);

create table if not exists tasks (
  id          integer  primary key,
  job         integer  not null references jobs(id),
  source      integer  not null references data(id),
  target      integer  not null references data(id) check (source != target),
  script      text     not null,
  dependency  integer  null references tasks(id) check (dependency is null or dependency != id),

  unique (job, target),
  unique (job, source, target)
);

create index if not exists tasks_id on tasks(id);
create index if not exists tasks_job on tasks(job);

create table if not exists attempts (
  task       integer  references tasks(id),
  attempt    integer  not null,
  start      integer  not null default (strftime('%s', 'now')),
  finish     integer  check (finish is null or finish >= start),
  exit_code  integer  default (null),

  primary key (task, attempt)
);

create index if not exists attempts_task on attempts(task);
create index if not exists attempts_attempt on attempts(task, attempt);
create index if not exists attempts_succeeded on attempts(task, attempt, exit_code) where exit_code = 0;
create index if not exists attempts_failed on attempts(task, attempt, exit_code) where exit_code is not null and exit_code != 0;

-- TODO Use recursive CTEs?
create view if not exists to_transfer as
  with failed as (
    select   attempts.task,
             count(attempts.attempt) as attempts
    from     attempts
    where    attempts.exit_code is not null
    and      attempts.exit_code != 0
    group by attempts.task
  ),
  succeeded as (
    select   attempts.task,
             count(attempts.attempt) as attempts
    from     attempts
    where    attempts.exit_code = 0
    group by attempts.task
  ),
  todo as (
    select    failed.task as task,
              failed.attempts
    from      failed
    left join succeeded
    on        succeeded.task      = failed.task
    and       succeeded.attempts >= failed.attempts
    where     succeeded.task is null

    union all

    select    tasks.id as task,
              0 as attempts
    from      tasks
    left join attempts
    on        attempts.task = tasks.id
    where     attempts.task is null
  )
  select    ready.task,
            tasks.job,
            source.filesystem as source_filesystem,
            source.address   as source_location,
            target.filesystem as target_filesystem,
            target.address   as target_location,
            tasks.script
  from      todo as ready
  join      tasks
  on        tasks.id = ready.task
  left join succeeded
  on        succeeded.task = tasks.dependency
  join      jobs
  on        jobs.id = tasks.job
  join      data as source
  on        source.id = tasks.source
  join      data as target
  on        target.id = tasks.target
  where     (tasks.dependency is null or succeeded.task is not null)
  and       ready.attempts < jobs.max_attempts;

commit;

-- Testing
begin exclusive transaction;

insert into jobs(max_attempts, max_concurrency) values (3, 10);

insert into data(filesystem, address) values
  ("xyzzy", "foo"),
  ("xyzzy", "bar"),
  ("xyzzy", "quux");

insert into tasks(job, source, target, script, dependency) values
  (1, 1, 2, "abc123", null),
  (1, 2, 3, "123abc", 1);

.print # Test 1
-- Expected: 1|1|xyzzy|foo|xyzzy|bar|abc123
select * from to_transfer;

.print # Test 2
-- Expected: <nothing>
insert into attempts(task, attempt) values (1, 1);
select * from to_transfer;

.print # Test 3
-- Expected: 1|1|xyzzy|foo|xyzzy|bar|abc123
update attempts set exit_code=1 where task = 1;
select * from to_transfer;

.print # Test 4
-- Expected: 2|1|xyzzy|bar|xyzzy|quux|abc
insert into attempts(task, attempt, exit_code) values (1, 2, 0);
select * from to_transfer;

.print # Test 5
-- Expected: 1|1|xyzzy|foo|xyzzy|bar|abc123
update attempts set exit_code = 1 where task = 1;
select * from to_transfer;

.print # Test 6
-- Expected: <nothing>
insert into attempts(task, attempt, exit_code) values (1, 3, 1);
select * from to_transfer;

.print # Test 7
-- Expected: 2|1|xyzzy|bar|xyzzy|quux|123abc
update attempts set exit_code = 0 where task = 1 and attempt = 3;
select * from to_transfer;

.print # Test 8
-- Expected: <nothing>
insert into attempts(task, attempt, exit_code) values (2, 1, 0);
select * from to_transfer;

rollback;
