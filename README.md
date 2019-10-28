# shepherd

`shepherd` is a library and CLI tool for copying data between different
filesystems -- either directly or via any number of intermediary stages,
as necessary -- in parallel, over a distributed environment.

## CLI Tool

<!-- TODO -->

    shepherd help
    shepherd [OPTIONS] QUERY
    shepherd [OPTIONS] --route=ROUTE [ROUTE OPTIONS]

### Automatic Routing

<!-- TODO -->

### Named Routes

<!-- TODO -->

### Configuration

By default, the settings for `shepherd` are read from `.shepherdrc`, in
your home directory, and its configuration is read from
[YAML](https://yaml.org/) files found in `.shepherd`, again in your home
directory. Either of these locations can be overridden with the
following common command line arguments:

    -S  --settings       FILE        Path to your .shepherdrc file   [~/.shepherdrc]
    -C  --configuration  DIR | FILE  Path to shepherd configuration  [~/.shepherd]

The `shepherd` configuration can either be a directory, from which all
YAML files will be read, or a specific YAML file. This argument can be
specified multiple times, where latter configuration will override any
that has been previously consumed.

**Note** It is important that the settings and configuration files are
available, at the same paths, on all the nodes of your distributed
environment.

#### `.shepherdrc`

<!-- TODO -->

#### `shepherd` Configuration

##### Filesystems

The list of available filesystems is specified under the `filesystems`
name, with the following schema for each element:

```yaml
name: [string]
driver: [string]
options:
  [parameter]: [value]
  # etc.
  max_concurrency: [int]
```

The `name` provides a reference when constructing [transfer
routes](#transfer-routes), using the `driver` and its optional
`options`. The available filesystem drivers and their options can be
found with:

    shepherd help filesystems

Note that `max_concurrency` is common to all filesystem drivers and has
an implementation-specific default, if not provided, which is listed in
the above help.

##### Transformers

The schema for transfer route transformers has the following schema:

```yaml
name: [string]
options:
  [parameter]: [value]
  # etc.
```

The `name` of available transformers, with their `options` (if any),
can be found with:

    shepherd help transformers

##### Transfer Routes

The list of valid transfer routes is specified under the `transfers`
name, with the following schema for each element:

```yaml
name: [string]
source: [filesystem]
target: [filesystem]
transformations:
  - [transformer]
  # etc.
template: [path | template]
cost: [int]
```

The `name` provides a reference, if used later in a [named
route](#named-routes). The `source` and `target`
[filesystems](#filesystems) must be defined, with `template` taking
either the path to a templated script, or the inlined script itself, to
perform the [transfer](#transfer-template). The list of
`transformations` is optional and are applied to the transfer route in
the order in which they are presented. The optional `cost` is the degree
of polynomial, temporal complexity for the transfer (i.e., the k in
O(n^k), where n ranges over the number of files), which defaults to 1
(i.e., linear time).

**Note** While transfer routes can have transformers applied to them,
they cannot be parametrised. They are considered fixed.

##### Transfer Template

The transfer template is a [Jinja2](https://palletsprojects.com/p/jinja/)
templated script that will be run over each file to perform the specific
transfer. It has available to it two variables -- `source` and `target`
-- which have attributes of `address` and `filesystem`.

For example:

```bash
#!/usr/bin/env bash
echo "Copying from {{ source.filesystem }} to {{ target.filesystem }}"
cp "{{ source.address | sh_escape }}" "{{ target.address | sh_escape }}"
```

For a list of template filters, see:

    shepherd help templating

##### Named Routes

Named routes are specific routes through the transfer graph to perform a
defined action. They allow each part of the route to be further
augmented with parametrisable transformations, which can be controlled
from the command line. They take the schema, under the `named_routes`
name:

```yaml
name: [string]
route:
- name: [transfer route]
  transformations:
  - [transformer]
  # etc.
# etc.
```

**Note** The `route` is a list of defined [transfer
routes](#transfer-routes), which must have the property that:

    route[n + 1].source == route[n].target

...for n > 0.

The list of `transformations` for each part of the route is optional and
the value for their options can be templated using Jinja2 syntax. All
variables used in the route will be passed in from the environment and
command line (with the latter taking precedence).

For example, the following named route:

```yaml
named_routes:
- name: backup
  route:
  - name: posix_to_tape
    transformations:
    - name: strip_common_path
    - name: prefix
      options:
        path: "/backup/{{ prefix_dir }}/{{ backup_date }}"
```

...could be invoked with:

    export prefix_dir="some_backup"
    shepherd --route=backup backup_date="$(date +%Ymd)" /path/to/backup

A list of named routes and their parametrised variables can be found
with:

    shepherd help named_routes

## Library

<!-- TODO Library documentation here... -->
