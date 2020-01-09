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

from ... import types as T
from ...exceptions import NOT_IMPLEMENTED
from .types import DataGenerator, BaseFilesystem


# TODO Obviously... :P
class iRODSFilesystem(BaseFilesystem):
    """ Filesystem implementation for iRODS filesystems """
    def __init__(self, *, name:str = "iRODS", max_concurrency:int = 10) -> None:
        self._name = name
        self.max_concurrency = max_concurrency

    def _accessible(self, address:T.Path) -> bool:
        # TODO This must be implemented at a minimum
        raise NOT_IMPLEMENTED

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
        # TODO This must be implemented at a minimum
        raise NOT_IMPLEMENTED

    def _size(self, address:T.Path) -> int:
        # TODO This must be implemented at a minimum
        raise NOT_IMPLEMENTED

    def set_metadata(self, address:T.Path, **metadata:str) -> None:
        raise NOT_IMPLEMENTED

    def delete_metadata(self, address:T.Path, *keys:str) -> None:
        raise NOT_IMPLEMENTED

    def delete_data(self, address:T.Path) -> None:
        raise NOT_IMPLEMENTED
