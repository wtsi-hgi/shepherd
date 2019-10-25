"""
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
"""

import re
from dataclasses import dataclass

from common import types as T, time


@dataclass
class LSFQueue:
    """ LSF queue model """
    name:str
    runlimit:T.Optional[T.TimeDelta] = None
    # TODO Other queue properties...


# Parsers for LSF parameter values
_HHMM = re.compile(r"(?P<hours>\d+):(?P<minutes>\d{2})")

def _parse_runlimit(value:str) -> T.TimeDelta:
    if value.isnumeric():
        return time.delta(minutes=int(value))

    # TODO In Python 3.8, use the new walrus operator
    hhmm = _HHMM.search(value)
    return time.delta(hours=int(hhmm["hours"]), minutes=int(hhmm["minutes"]))

# Patterns to match in lsb.queues
_COMMENT = re.compile(r"^\s*#")
_BEGIN   = re.compile(r"Begin Queue")
_END     = re.compile(r"End Queue")
_SETTING = re.compile(r"(?P<key>\w+)\s*=\s*(?P<value>.+)\s*$")

# Map from LSF parameter to LSFQueue attribute, with respective parser
_MAPPING = {
    "QUEUE_NAME": ("name",     None),
    "RUNLIMIT":   ("runlimit", _parse_runlimit)}

def parse_config(config:T.Path) -> T.Dict[str, LSFQueue]:
    """
    Parse lsb.queues file to extract queue configuration

    @param   config  Path to lsb.queues
    @return  Dictionary of parsed queue parameters
    """
    queues = {}

    in_queue_def = False
    this_queue:T.Dict[str, T.Any] = {}
    with config.open(mode="rt") as f:
        for line in f:
            if _COMMENT.match(line):
                continue

            if _BEGIN.search(line):
                in_queue_def = True
                continue

            if _END.search(line):
                in_queue_def = False

                name         = this_queue["name"]
                queues[name] = LSFQueue(**this_queue)
                this_queue   = {}

                continue

            # TODO In Python 3.8, use the new walrus operator
            option = _SETTING.search(line)
            if in_queue_def and option is not None:
                key, value = option["key"], option["value"]

                if key in _MAPPING:
                    mapped_key, value_mapper = _MAPPING[key]
                    this_queue[mapped_key] = (value_mapper or str)(value)

    return queues
