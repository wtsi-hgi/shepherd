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
  job         integer  references jobs(id),
  filesystem  text     not null,
  location    text     not null,

  unique (job, filesystem, location)
);

create index if not exists data_pk on data(id);
create index if not exists data_key on data(id, job);
create index if not exists data_location on data(job, filesystem, location);

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
  source      integer  references data(id),
  target      integer  references data(id) check (source != target),
  script      text     not null,
  dependency  integer  null references tasks(id) check (dependency is null or dependency != id),

  unique (target),
  unique (source, target)
);

create index if not exists tasks_pk on tasks(id);

create table if not exists attempts (
  task       integer  primary key references tasks(id),
  attempt    integer  not null,
  start      integer  not null default (strftime('%s', 'now')),
  finish     integer  check (finish is null or finish >= start),
  exit_code  integer  default (null),

  unique (task, attempt)
) without rowid;

create index if not exists attempts_task on attempts(task);
create index if not exists attempts_attempt on attempts(task, attempt);

commit;
