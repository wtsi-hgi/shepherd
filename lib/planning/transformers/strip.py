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

from os.path import commonpath
import json
import base64 
import re

from common import types as T
from common.logging import log
from common.models.filesystems.types import Data
from ..types import RouteIOTransformation, IOGenerator


_ROOT = T.Path("/")


def _strip_common_prefix(io:IOGenerator) -> IOGenerator:
    """ Strip the common prefix from all target locations """
    _buffer:T.List[T.Tuple[Data, Data]] = []
    _prefix:T.Optional[Data] = None


    for source, target in io:
        # We calculate the common prefix one location at a time, because
        # os.path.commonpath otherwise eats a lot of memory
        _buffer.append((source, target))
        _prefix = T.Path(commonpath((_prefix or target.address, target.address)))

    for source, target in _buffer:
        new_target = Data(
            filesystem = target.filesystem,
            address    = _ROOT / target.address.relative_to(_prefix))

        yield source, new_target


strip_common_prefix = RouteIOTransformation(_strip_common_prefix)


def _vault_transformer(io:IOGenerator) -> IOGenerator:
    """ Transformer for vault."""
    _buffer:T.List[T.Tuple[Data, Data, Data]] = []
    _prefix:T.Optional[Data] = None

    def _find_volume_name(source: T.Path):
        '''Extracts the project name from the given vault file path by scanning for parent of .vault directory. E.g.: /path/to/my-project/.vault/.staged/01/23/45/67/89/ab-Zm9vL2Jhci9xdXV4 should return my-project'''
        source = str(source)
        pattern = re.compile("scratch[0-9]+")
        result = re.findall(pattern, source)
        if len(result):
            log.debug(f"Volume found in source {source}: {result}")
            return result[0]
        else:
            log.critical(f"Source {source} does not have any scratch disks in its path!")
            return "."

    def _find_project_group(source: T.Path) -> str:
        '''Extracts the unix group of the project. E.g.: /path/to/my-project/.vault/.staged/01/23/45/67/89/ab-Zm9vL2Jhci9xdXV4 should return the unix group that owns my-project'''
        source = str(source)
        end_index =  source.find("/.vault")
        if end_index == -1:
            log.critical(f"Given source path {source} does not seem to be a valid vault file path")
        project_prefix = source[0: end_index]

        project_prefix = T.Path(project_prefix)
        if project_prefix.exists():
            return project_prefix.group()
        else:
            raise Exception(f"Project Prefix Path {project_prefix} not found")


    def _find_decoded_relative_path(source: T.Path) -> str:
        '''Extracts the base 64 decoded file path'''
        source = str(source)
        start_index = source.rfind("-") + 1
        if (start_index == 0):
            log.critical(f"Given source path {source} does not seem to be a valid vault file path")
        b64encoded_path = source[start_index:]
        b64decoded_path = base64.b64decode(b64encoded_path)
        return b64decoded_path.decode("utf-8")
    
    for source, target in io:  
        group_mapping = {}
        script_dir = T.Path(__file__).parent
        with open(script_dir/ 'teams.json', 'r') as json_file:
            group_mapping = json.load(json_file)
        _group = _find_project_group(source.address) 
        _project = group_mapping.get(_group, _group)
        _volume = _find_volume_name(source.address)
        _decoded_vault_relative_path = _find_decoded_relative_path(source.address)
        _buffer.append((source, target, _volume, _project, _decoded_vault_relative_path))
        # _prefix = T.Path(commonpath((_prefix or target.address, target.address)))
    for source, target, volume, project, vault_relative_path in _buffer:
        address    = _ROOT /  project / volume / vault_relative_path
        log.debug(f"Transforming Route. Source: {source.address} Target: {target.address} project: {project}, volume: {volume}, relative path: {vault_relative_path} Final Address: {address}") 
        new_target = Data(
            filesystem = target.filesystem,
            address    = address)
       
        yield source, new_target


vault_transformer = RouteIOTransformation(_vault_transformer)



def last_n_components(n:int) -> RouteIOTransformation:
    """
    Route IO transformation factory that takes, at most, the last n
    components from the target location
    @param   n  Number of components
    @return  IO transformer
    """
    assert n > 0

    def _last_n(io:IOGenerator) -> IOGenerator:
        for source, target in io:
            new_target = Data(
                filesystem = target.filesystem,
                address    = _ROOT / T.Path(*target.address.parts[-n:]))

            yield source, new_target

    return RouteIOTransformation(_last_n)



