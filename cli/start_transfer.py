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

from cli.yaml_parser import read_yaml
from lib.planning.types import TransferRoute, PolynomialComplexity, FilesystemVertex

class QueryError(Exception):
    """Raised when an unrecognised query is received from the user"""

def parse_action(action:T.List[str]) -> T.Dict[str, T.Any]:
    """Parse action input and return dictionary of relevant values."""
    query:T.Dict[str, T.Any] = {}

    if action[0] == "through" and action[2] == "using":
        query["route"] = action[1]
        query["fofn"] = action[3]
    elif action[1] == "from" and action[3] == "to" and action[5] == "using":
        query["source"] = action[2]
        query["target"] = action[4]
        query["fofn"] = action[6]
    else:
        raise QueryError(f"Query '{' '.join(action)}' not recognised.")

    return query

def start_transfer(action:T.List[str], config:T.Dict[str, T.Any]) -> None:
    """
    Starts the shepherd file transfer process based on user input and program
    configuration.

    @param action List of user input strings
    @param config Dictionary of various shepherd configuration values
    """
    transfer_objects = read_yaml(config["configuration"], config["variables"])

    query = parse_action(action)

    if "route" in query.keys():
        try:
            route = transfer_objects["named_routes"][query["route"]]
        except KeyError:
            raise QueryError(f"Named route '{query['route']}' is not defined in the configuration file.")
    elif "source" in query.keys() and "target" in query.keys():
        try:
            source = FilesystemVertex(
                transfer_objects["filesystems"][query["source"]] )
            target = FilesystemVertex(
                transfer_objects["filesystems"][query["target"]] )
            # TODO: What's the transfer route for these objects?
            # What's the template, cost, etc
        except KeyError:
            raise QueryError(f"Either '{query['source']}' ")
