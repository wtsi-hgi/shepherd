# shepherd

**Work In Progress: This project is currently under active development
and not in a released or usable state.**

`shepherd` is a library and CLI tool for copying data between different
filesystems -- either directly or via any number of intermediary stages,
as necessary -- in parallel, over a distributed environment.

## CLI Tool

The standard usage pattern takes a routing clause and a targeting query:

    shepherd [OPTIONS] ROUTING QUERY

These are described herein. Otherwise, help is always available with:

    shepherd [OPTIONS] help [SUBJECT]

<!-- TODO: More operation modes, such as reporting, restarting, etc. -->

### Automatic Routing

The routing clause for automatic routing takes the form:

    from FILESYSTEM to FILESYSTEM

Where each `FILESYSTEM` is [defined](#filesystems) in the `shepherd`
configuration. Provided there is a valid [route](#transfer-routes)
between these two filesystems, then `shepherd` will follow it;
otherwise, the process will fail.

### Named Routes

The routing clause for [named routes](#named-routes-1) takes the form:

    through ROUTE

Where `ROUTE` is a valid named route in the `shepherd` configuration.

### Targeting Query

The targeting query is used to identify files for the transfer. It is
expressed in a simple DSL, described herein.

#### Examples

* Files from a file of filenames:

      take from /path/to/fofn

* All `.tar.gz` files under the current working directory and `/tmp`,
  which either haven't been modified in the last 90 days or are larger
  than 1GB:

      take . /tmp where (mtime >= 90 days or size >= 1GB) and name = "*.tar.gz"

* Files from a file of filenames, with the given "reference" metadata
  value:

      take from /path/to/fofn where :reference = GRCh38

**Note** Special characters used by the shell ought to be quoted to
avoid their automatic expansion. If you need to quote any values in the
context of the query, then those (double) quotes either need to be
escaped, or be enquoted within single quotes at the shell-level.

**Note** Some filesystems may not support certain forms of query and
their use will result in error.

#### Grammar

**Note** Tokens are considered to be separated by at least one
whitespace (`1*WSP`) in the following definitions. If tokens need to be
juxtaposed (i.e., without whitespace), then they will be interposed in
the grammar by an ampersand; if whitespace is optional (`*WSP`), tokens
will be joined by a question mark; these same sigils will be prefixed to
repetition rules wherever it's ambiguious:

    TOKEN TOKEN    ; foo bar
    TOKEN & TOKEN  ; foobar
    TOKEN ? TOKEN  ; foobar / foo bar
    TOKEN &1*TOKEN ; foofoofoofoo

Otherwise, the grammar definition will be in ABNF, per [RFC
5234](https://tools.ietf.org/html/rfc5234):

```abnf
QUERY       =  "take" SOURCE [CRITERIA]

SOURCE      =  FOFN / ROOTS

FOFN        =  "from" PATH ["compressed"] ["delimited" "by" OCTET]
            ;  File of filenames

ROOTS       =  1*PATH
            ;  Directory tree root(s)

PATH        =  ; TODO
            ;  POSIX path (relative or absolute)

CRITERIA    =  "where" EXPRESSION

EXPRESSION  =  "(" ? EXPRESSION ? ")" / PREDICATE *(CONNECTIVE EXPRESSION)

PREDICATE   =  [NEGATION] KEYWORD ? COMPARATOR ? VALUE

KEYWORD     =  ATTRIBUTE / METADATA

ATTRIBUTE   =  "size"
ATTRIBUTE   =/ "name" / "path"
ATTRIBUTE   =/ "mtime" / "ctime" / "atime"
ATTRIBUTE   =/ "depth"
ATTRIBUTE   =/ "owner" / "group"
            ;  TODO Others?...

METADATA    =  ":" & ALPHA & &*(ALPHA / DIGIT / "_")

COMPARATOR  =  "=" / ">" / ">=" / "<" / "<="

VALUE       =  STRING [UNIT]

STRING      =  DQUOTE & &1*VCHAR & DQUOTE / &1*VCHAR
            ;  FIXME This is not exact; expand to cover, e.g., escaping

UNIT        =  SIZE-UNIT / TIME-UNIT

SIZE-UNIT   =  (%x6b / %x4d / %x47 / %x54 / %x50) & [%x69] & [%x42]
            ;  k/M/G/T/P & [i] & [B]

TIME-UNIT   =  ("hour" / "day" / "week" / "year") & ["s"]

NEGATION    =  "not"

CONNECTIVE  =  "and" / "or"
```

#### Context and Semantics

<!-- TODO source filesystem, support (e.g., metadata), precedence,
globbing -->

### Configuration

By default, the configuration for `shepherd` is read from the
[YAML](https://yaml.org) files contained in `.shepherd`, in your home
directory. This location can be overridden with the following, common
command line argument:

    -C  --configuration=DIR|FILE  Path to shepherd configuration  [~/.shepherd]

The `shepherd` configuration can either be a directory, from which all
YAML files (i.e., with extension `.yml` or `.yaml`) will be read, or a
specific YAML file. This argument can be specified multiple times, where
latter configuration will override any that has been previously
consumed. (No precedence action is defined in the case of a tie.)

**Note** It is important that all configuration files are available, at
the same path, on all the nodes of your distributed environment that
need them.

#### Templating

The `shepherd` configuration may contain templated values, where
specified, using [Jinja2](https://palletsprojects.com/p/jinja/) syntax.
These will be reified at runtime using values for template variables
taken from the following sources, in the given precedence:

1. Command line arguments, which can be specified multiple times:

       -v VARIABLE=VALUE

2. Environment variables, prefixed with `SHEPHERD_`;

3. Variable definition YAML files, specified as command line arguments,
   which again may be specified multiple times:

       --variables=/path/to/variables.yml

4. Variables defined under the `defaults` name in any configuration YAML
   files.

When the same variable is defined in multiple sources, then the most
recent will be taken from the highest priority source, per the above.

**Note** `source` and `target` are reserved variables and _must not_ be
specified. Any attempt to will result in an error.

**Note** Any variables that are used in templates, but _not_ specified
at runtime will result in error. All used variables _must_ be defined.

For the list of used variables, for the given configuration, and
available Jinja2 filters, see:

    shepherd help templating

#### Filesystems

The list of available filesystems is specified under the `filesystems`
name, with the following schema for each element:

```yaml
name: <string>
driver: <string>
options:
  <parameter>: <value>
  # etc.
  max_concurrency: <int>
```

The `name` provides a reference when constructing [transfer
routes](#transfer-routes), using the `driver` and its optional
`options`. The available filesystem drivers and their options can be
found with:

    shepherd help filesystems

Note that `max_concurrency` is common to all filesystem drivers and has
an implementation-specific default, if not provided, which is listed in
the above help.

#### Transformers

Transformers are not root level objects, but are used in the definition
of [transfer routes](#transfer-routes) and [named
routes](#named-routes-1). They have the following schema:

```yaml
name: <string>
options:
  <parameter>: <templated value>
  # etc.
```

The `name` of available transformers, with their `options` (if any),
can be found with:

    shepherd help transformers

The `value` for each parameter may be templated using Jinja2 syntax.

#### Transfer Routes

The list of valid transfer routes is specified under the `transfers`
name, with the following schema for each element:

```yaml
name: <string>
source: <filesystem>
target: <filesystem>
transformations:
  - <transformer>
  # etc.
template: <path | template>
cost: <int>
```

The `name` provides a reference, if used later in a [named
route](#named-routes-1). The `source` and `target`
[filesystems](#filesystems) must be defined, with `template` taking
either the path to a templated script, or the inlined script itself, to
perform the [transfer](#transfer-template). The list of
`transformations` is optional and are applied to the transfer route in
the order in which they are presented. The optional `cost` is the degree
of polynomial, temporal complexity for the transfer (i.e., the k in
O(n<sup>k</sup>), where n ranges over the number of files), which
defaults to 1 (i.e., linear time).

<!-- TODO
A visualisation of the complete transfer graph can be obtained with:

    shepherd help transfers
-->

#### Transfer Template

The transfer template is a Jinja2 templated script that will be run over
each file to perform the specific step of the transfer. It has available
to it two special variables -- `source` and `target` -- which have
attributes of `address` and `filesystem`.

For example:

```bash
#!/usr/bin/env bash
echo "Copying from {{ source.filesystem }} to {{ target.filesystem }}"
cp "{{ source.address | sh_escape }}" "{{ target.address | sh_escape }}"
```

The [full spectrum of template variables and filters](#templating) will
also be available.

#### Named Routes

Named routes are specific routes through the transfer graph to perform a
defined action. They allow each part of the route to be further
augmented with additional, parametrisable transformations. They take the
schema, under the `named_routes` name:

```yaml
name: <string>
route:
- name: <transfer route>
  transformations:
  - <transformer>
  # etc.
# etc.
```

**Note** The `route` is a list of defined [transfer
routes](#transfer-routes), which must have the property that, for n > 0:

    route[n + 1].source == route[n].target

The list of `transformations` for each part of the route is optional and
the value for their options can be templated using Jinja2 syntax. All
variables used in the route will be passed in at runtime and _must_ be
specified.

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

    export SHEPHERD_prefix_dir="some_backup"
    shepherd -v backup_date="$(date +%Ymd)" through backup take /path/to/backup

A list of named routes and their parametrisable variables, for the given
configuration, can be found with:

    shepherd help routes

#### Execution

The setup and configuration of the execution engine is defined under the
`executor` and `phase` names, which allows the distributed phases of the
process to be configured independently.

The `executor` configuration takes the following schema:

```yaml
driver: <executor>
options:
  <parameter>: <templated value>
  # etc.
```

The list of valid execution drivers, their setup parameters and runtime
configuration options can be found with:

    shepherd help execution

The distributed phases of the process can be found with:

    shepherd help phases

Runtime execution configuration for the phases may be templated. For
example:

```yaml
executor:
  driver: LSF
  options:
    config_dir: /path/to/lsf/{{ cluster }}/config

phase:
  preparation:
    group: "{{ group }}"
    cores: 1
    memory: 100

  transfer:
    group: "{{ group }}"
    cores: 4
    memory: 1000
```

<!-- TODO

#### State

-->

## Library

<!-- TODO Library documentation here... -->
