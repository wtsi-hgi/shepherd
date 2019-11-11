"""
Copyright (c) 2019 Genome Research Limited

Author: Filip Makosza <fm12@sanger.ac.uk>

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
"""

from common import types as T

class InvalidArgumentError(Exception):
    """Raised when an argument input by the user is not recognised."""

def general_help():
    """Prints general shepherd usage help text."""
    print(
    """
shepherd is a CLI tool for copying data between different file systems - either
directly or via any number of intermediary stages, as necessary - in parallel,
over a distributed environment.

Standard usage:
    shepherd [OPTIONS] ROUTING QUERY
    shepherd [OPTIONS] help [SUBJECT]

Available help subjects are:"""
    )
    for i in available_topics:
        print(f"- {i}")

def template_help(config:T.Dict[str, T.Any]) -> None:
    """Print template help text."""
    # TODO: Add help text
    pass

def filesystem_help():
    """Prints filesystem help text."""
    # TODO: Add help text
    pass

def transformer_help():
    """Prints transformer help text."""
    # TODO: Add help text
    pass

def route_help():
    """Prints route help text."""
    # TODO: Add help text
    pass

def action_help():
    """Prints action help text."""
    print("""
Submit an action to shepherd. The action can either be a simple command, such
as 'help', or a transfer query describing the transfer you want shepherd to
carry out.

Actions:
    shepherd [OPTIONS] help [TOPIC]

Queries:
    shepherd [OPTIONS] through [ROUTE] using [FOFN PATH]
    shepherd [OPTIONS] from [FILE SYSTEM] to [FILE SYSTEM] using [FOFN PATH]
    """)

def executor_help():
    """Prints executor help text."""
    # TODO
    pass

def phase_help():
    # TODO: better description of phases
    print("""
Phases describe the parameters passed to the execution engine at different
stages of shepherd's execution. They are defined in the configuration file.

phase:
  (phase_name):
    group:
    cores:
    memory:
    queue:

There are two mandatory phase definitions:
- preparation: Job which prepares the transfer jobs, collating files and instantiating the state engine
- transfer: Jobs which transfer files from one filesystem to another
    """)

available_topics:T.Dict[str, T.Callable] = {
    "templating": template_help,
    "filesystems": filesystem_help,
    "transformers": transformer_help,
    "routes": route_help,
    "actions": action_help,
    "executors": executor_help,
    "phases": phase_help
}

def help(topics:T.List[str]) -> None:
    """
    Main helper function. Takes a help topic and prints a relevant
    help string to standard output.

    @param topics List of arguments including the 'help' keyword.
    """

    if len(topics) == 1:
        general_help()
        exit()

    topics = topics[1:]

    for topic in topics:
        if topic not in available_topics:
            print(f"Help is not available for {topic}. Available topics include:")
            for i in available_topics:
                print(f"- {i}")
            exit()

    for topic in topics:
        available_topics[topic]()
    exit()
