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

from common import types as T
from common.exceptions import NOT_IMPLEMENTED
from ..transfer import FilesystemVertex, DataLocation, DataGenerator

# TODO Obviously... :P
class iRODSFilesystem(FilesystemVertex):
    """ Filesystem vertex implementation for iRODS filesystems """
    _name = "iRODS"

    def _accessible(self, data:DataLocation) -> bool:
        raise NOT_IMPLEMENTED

    def _identify_by_metadata(self, **metadata:str) -> DataGenerator:
        raise NOT_IMPLEMENTED

    def _identify_by_stat(self, path:DataLocation, *, name:str = "*") -> DataGenerator:
        raise NOT_IMPLEMENTED

    def _identify_by_fofn(self, fofn:DataLocation, *, delimiter:str = "\n", compressed:bool = False) -> DataGenerator:
        raise NOT_IMPLEMENTED

    @property
    def supported_checksums(self) -> T.List[str]:
        return ["md5"]

    def _checksum(self, algorithm:str, data:DataLocation) -> str:
        raise NOT_IMPLEMENTED

    def _size(self, data:DataLocation) -> int:
        raise NOT_IMPLEMENTED

    def set_metadata(self, data:DataLocation, **metadata:str) -> None:
        raise NOT_IMPLEMENTED

    def delete_metadata(self, data:DataLocation, *keys:str) -> None:
        raise NOT_IMPLEMENTED

    def delete_data(self, data:DataLocation) -> None:
        raise NOT_IMPLEMENTED
