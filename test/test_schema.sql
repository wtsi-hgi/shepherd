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

-- TODO Rig this up in a suitable testing harness

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
select * from todo;

.print # Test 2
-- Expected: <nothing>
insert into attempts(task, attempt) values (1, 1);
select * from todo;

.print # Test 3
-- Expected: 1|1|xyzzy|foo|xyzzy|bar|abc123
update attempts set exit_code=1 where task = 1;
select * from todo;

.print # Test 4
-- Expected: 2|1|xyzzy|bar|xyzzy|quux|123abc
insert into attempts(task, attempt, exit_code) values (1, 2, 0);
select * from todo;

.print # Test 5
-- Expected: 1|1|xyzzy|foo|xyzzy|bar|abc123
update attempts set exit_code = 1 where task = 1;
select * from todo;

.print # Test 6
-- Expected: <nothing>
insert into attempts(task, attempt, exit_code) values (1, 3, 1);
select * from todo;

.print # Test 7
-- Expected: 2|1|xyzzy|bar|xyzzy|quux|123abc
update attempts set exit_code = 0 where task = 1 and attempt = 3;
select * from todo;

.print # Test 8
-- Expected: <nothing>
insert into attempts(task, attempt, exit_code) values (2, 1, 0);
select * from todo;

rollback;
