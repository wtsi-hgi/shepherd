"""
Copyright (c) 2020 Genome Research Limited

Authors:
* Christopher Harrison <ch12@sanger.ac.uk>
* Piyush Ahuja <pa11@sanger.ac.uk>

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

import base64
import json
import re
import importlib.resources as resource

from common import types as T
from common.logging import log
from common.models.filesystems.types import Data
from ..types import RouteIOTransformation, IOGenerator

# Vault paths can take six forms:
#
# Project Directory
#   /lustre/scratch101/projects/my_project/.vault/.staged/01/23/45/67/89/ab-Zm9vL2Jhci9xdXV4
#   /lustre/scratch101/realdata/mdt0/projects/my_project/.vault/.staged/01/23/45/67/89/ab-Zm9vL2Jhci9xdXV4
#
# Team Directory
#   /lustre/scratch101/teams/my_team/.vault/.staged/01/23/45/67/89/ab-Zm9vL2Jhci9xdXV4
#   /lustre/scratch101/realdata/mdt0/teams/my_team/.vault/.staged/01/23/45/67/89/ab-Zm9vL2Jhci9xdXV4
#
# HGI Team Directory (special case)
#   /lustre/scratch101/.vault/.staged/01/23/45/67/89/ab-Zm9vL2Jhci9xdXV4
#   /lustre/scratch101/realdata/mdt0/.vault/.staged/01/23/45/67/89/ab-Zm9vL2Jhci9xdXV4
#
# We are interested in:
# 1. The base64 encoded path, at the end (n.b., This uses a non-standard
#    base64 alphabet, replacing the usual "/" character with an "_")
# 2. The group, which is the group owner of the directory immediately
#    above the .vault directory (appropriately mapped)
# 3. Whether this is a project or team directory, which can be deduced
#    by the directory immediately above the group directory
# 4. The Lustre volume, which is the directory immediately after /lustre
#
# Note that, because of the special case of the HGI team directory on a
# Lustre volume with a single MDT (i.e., group and Lustre volume are the
# same), we can't use a single regular expression to factor out the
# parts. However, the Lustre volume is always the second level from the
# root, so it's trivial to extract.

_HUMGEN = T.Path("/humgen")

_TEAM_MAPPING = json.loads(
    resource.read_text("lib.planning.transformers", "teams.json"))

_VAULT_PATH = re.compile(r"""
  ^                               # Start of string
  (?P<prefix>                     # Full group directory
    .*?/
    (?P<type> [^/]+ )/            # The 'type' directory
    (?P<group> [^/]+ )            # The group directory
  )/
  \.vault/[^/]+                   # The vault directory
  (?:/[0-9a-f]{2})*/[0-9a-f]{2}   # The encoded inode
  -                               # Delimiter
  (?P<path>[a-zA-Z0-9+_]+={0,2})  # The base64 encoded path
  $                               # End of string
""", re.VERBOSE)

def _decode(encoded:str) -> str:
    return base64.b64decode(encoded, altchars=b"+_").decode()

def _vault_transformer(io:IOGenerator) -> IOGenerator:
    for source, target in io:
        match = _VAULT_PATH.match(str(source.address))
        if match:
            # The group type is either "projects" or "teams"
            group_type = "projects" if match["type"] == "projects" else "teams"

            # The group is the mapping of the group directory's group
            # owner, if it exists, falling back to the name of the directory
            group_path = T.Path(match["prefix"])
            group = _TEAM_MAPPING.get(group_path.group(), match["group"])

            # The Lustre volume is the second component in the source
            # address (n.b. root is the zeroth component)
            lustre = source.address.parts[2]

            # We finally just need to decode the path
            decoded_path = _decode(match["path"])

            canonical_path = _HUMGEN / group_type / group / lustre / decoded_path
            log.debug(f"Vault address {source.address} maps to {canonical_path}")
            yield source, Data(
                filesystem = target.filesystem,
                address    = canonical_path)

        else:
            log.critical(f"{source.address} is not recognised as a Vault path")

vault_transformer = RouteIOTransformation(_vault_transformer)
