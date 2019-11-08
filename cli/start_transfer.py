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
from common.logging import log, Level
from common.models.graph import Graph, Edge
from lib.state.native.db import NativeJob, create_root
from lib.execution.lsf import LSF, LSFSubmissionOptions
from lib.planning.types import TransferRoute, PolynomialComplexity, FilesystemVertex

class QueryError(Exception):
    """Raised when an unrecognised query is received from the user"""

# TODO: implement these into the user configuration
_CLUSTER = "farm4"

#_EXEC = {
#    _CLUSTER: LSF(T.Path(f"/usr/local/lsf/conf/lsbatch/{_CLUSTER}/configdir"), name=_CLUSTER)
#}

def parse_action(action:T.List[str]) -> T.Dict[str, T.Any]:
    """Parse action input and return dictionary of relevant values."""
    # TODO: implement actual query language parser
    query:T.Dict[str, T.Any] = {}

    if action[0] == "through" and action[2] == "using":
        query["route"] = action[1]
        query["fofn"] = action[3]
    elif action[0] == "from" and action[2] == "to" and action[4] == "using":
        query["source"] = action[1]
        query["target"] = action[3]
        query["fofn"] = action[5]
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

    route:T.TransferRoute = None

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

            graph = Graph()
            edge = Edge(source, target)
            graph += edge
            route = graph.route(source, target)

        except KeyError:
            raise QueryError(f"Either '{query['source']}' or '{query['target']}' is not defined in the configuration file.")

    working_dir = create_root(T.Path("."))

def prepare_state_from_fofn():
    pass
