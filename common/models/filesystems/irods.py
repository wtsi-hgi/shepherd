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
import re
import shlex
import subprocess
from dataclasses import dataclass
from functools import lru_cache

from ... import types as T
from ...logging import log
from ...exceptions import NOT_IMPLEMENTED
from .types import DataGenerator, BaseFilesystem


# TODO This is a MINIMAL iRODS filesystem implementation


@lru_cache(maxsize=1)
def _baton(address:T.Path) -> T.SimpleNamespace:
    # Simple baton-list Wrapper
    query = json.dumps({
        "collection":  os.path.dirname(address),
        "data_object": os.path.basename(address)
    })

    hammer = True
    while hammer:
        baton = subprocess.run(
            shlex.split("baton-list --acl --size --checksum"),
            input          = query,
            capture_output = True,
            text           = True,
            check          = False)

        try:
            decoded = json.loads(baton.stdout)
            hammer = False

        except json.JSONDecodeError:
            # If we can't decode baton's output as JSON, then something
            # went wrong and we should try again
            # FIXME We should probably not keep trying indefinitely!
            pass

    return T.SimpleNamespace(**decoded)


_IRODS_NAME = re.compile(r"(?<=^name: )(?P<name>.+)$", re.MULTILINE)
_IRODS_ZONE = re.compile(r"(?<=^zone: )(?P<zone>.+)$", re.MULTILINE)

@dataclass(init=False)
class _iRODSUser:
    """ Current iRODS user model """
    name:str
    zone:str

    def __init__(self) -> None:
        iuserinfo = subprocess.run("iuserinfo", capture_output=True, check=True, text=True)

        search = _IRODS_NAME.search(iuserinfo.stdout)
        if search is not None:
            self.name = search["name"]

        search = _IRODS_ZONE.search(iuserinfo.stdout)
        if search is not None:
            self.zone = search["zone"]


_REQUIREMENTS = [
    ("baton-list", "baton is not available; see http://wtsi-npg.github.io/baton for details"),
    ("iuserinfo",  "icommands not found; see https://irods.org/download for details")
]

# Some iRODS error numbers
# https://github.com/irods/irods/blob/master/lib/core/include/rodsErrorTable.h
_IRODS_FILE_NOT_FOUND = -310000
_IRODS_ACCESS_DENIED  = -350000
_IRODS_NO_PERMISSION  = -818000

class iRODSFilesystem(BaseFilesystem):
    """ Filesystem implementation for iRODS filesystems """
    _irods_user:_iRODSUser

    def __init__(self, *, name:str = "iRODS", max_concurrency:int = 10) -> None:
        self._name = name
        self.max_concurrency = max_concurrency

        # Check that all our external dependencies are available
        for binary, help_text in _REQUIREMENTS:
            try:
                subprocess.run(
                    f"command -v {binary}",
                    stdout = subprocess.DEVNULL,
                    stderr = subprocess.DEVNULL,
                    check  = True,
                    shell  = True)

            except subprocess.CalledProcessError:
                log.critical(help_text)
                raise

        self._irods_user = _iRODSUser()

    def _accessible(self, address:T.Path) -> bool:
        baton = _baton(address)

        if hasattr(baton, "error"):
            if baton.error["code"] == _IRODS_FILE_NOT_FOUND:
                raise FileNotFoundError(f"File not found on iRODS: {address}")

            if baton.error["code"] in [_IRODS_ACCESS_DENIED, _IRODS_NO_PERMISSION]:
                return False

        acls = baton.access
        user = self._irods_user.name
        zone = self._irods_user.zone
        return any(user == acl["owner"]
                   and zone == acl["zone"]
                   and acl["level"] in ["read", "own"]
                   for acl in acls)

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
