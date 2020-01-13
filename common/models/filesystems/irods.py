"""
Copyright (c) 2019, 2020 Genome Research Limited

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

import json
import os.path
import shlex
import subprocess
from dataclasses import dataclass
from functools import lru_cache

from ... import types as T
from ...logging import log
from ...exceptions import NOT_IMPLEMENTED
from .types import DataGenerator, BaseFilesystem, UnsupportedByFilesystem


# TODO This is a MINIMAL iRODS filesystem implementation


@dataclass
class _BatonListOutput:
    collection:str
    data_object:str
    size:int
    checksum:str

@lru_cache(maxsize=1)
def _baton(address:T.Path) -> _BatonListOutput:
    # Simple baton-list Wrapper
    query = json.dumps({
        "collection":  os.path.dirname(address),
        "data_object": os.path.basename(address)
    })

    baton = subprocess.run(
        shlex.split("baton-list --size --checksum"),
        input          = query,
        capture_output = True,
        text           = True,
        check          = True)

    decoded = json.loads(baton.stdout)
    return _BatonListOutput(**decoded)


class iRODSFilesystem(BaseFilesystem):
    """ Filesystem implementation for iRODS filesystems """
    def __init__(self, *, name:str = "iRODS", max_concurrency:int = 10) -> None:
        self._name = name
        self.max_concurrency = max_concurrency

        # Check that baton is available
        try:
            subprocess.run(
                "command -v baton-list",
                stdout = subprocess.DEVNULL,
                stderr = subprocess.DEVNULL,
                check  = True,
                shell  = True)

        except subprocess.CalledProcessError:
            log.critical("baton is not available; see http://wtsi-npg.github.io/baton for details")
            raise

    def _accessible(self, address:T.Path) -> bool:
        # FIXME This should parse the error code returned by iRODS
        try:
            _baton(address)
            return True

        except:
            return False

    def _identify_by_metadata(self, **metadata:str) -> DataGenerator:
        raise NOT_IMPLEMENTED

    def _identify_by_stat(self, address:T.Path, *, name:str = "*") -> DataGenerator:
        raise NOT_IMPLEMENTED

    def _identify_by_fofn(self, fofn:T.Path, *, delimiter:str = "\n", compressed:bool = False) -> DataGenerator:
        raise NOT_IMPLEMENTED

    @property
    def supported_checksums(self) -> T.List[str]:
        return ["md5"]

    def _checksum(self, algorithm:str, address:T.Path) -> str:
        # NOTE algorithm will be MD5 by necessity
        return _baton(address).checksum

    def _size(self, address:T.Path) -> int:
        return _baton(address).size

    def set_metadata(self, address:T.Path, **metadata:str) -> None:
        raise NOT_IMPLEMENTED

    def delete_metadata(self, address:T.Path, *keys:str) -> None:
        raise NOT_IMPLEMENTED

    def delete_data(self, address:T.Path) -> None:
        raise NOT_IMPLEMENTED
