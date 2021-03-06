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

import gzip
import hashlib
import os

from ... import types as T
from ...constants import BLOCKSIZE
from ...exceptions import NOT_IMPLEMENTED
from .types import Data, DataGenerator, BaseFilesystem, UnsupportedByFilesystem


_NO_METADATA = UnsupportedByFilesystem("POSIX filesystems do not support key-value metadata")


class POSIXFilesystem(BaseFilesystem):
    """ Filesystem implementation for POSIX-like filesystems """
    def __init__(self, *, name:str = "POSIX", max_concurrency:int = 1) -> None:
        self._name = name
        self.max_concurrency = max_concurrency

    def _identify_by_metadata(self, **metadata:str) -> DataGenerator:
        raise _NO_METADATA

    def set_metadata(self, address:T.Path, **metadata:str) -> None:
        raise _NO_METADATA

    def delete_metadata(self, address:T.Path, *keys:str) -> None:
        raise _NO_METADATA

    def _accessible(self, address:T.Path) -> bool:
        return address.exists() and os.access(address, os.R_OK)

    def _identify_by_stat(self, address:T.Path, *, name:str = "*") -> DataGenerator:
        # TODO
        raise NOT_IMPLEMENTED

    def _identify_by_fofn(self, fofn:T.Path, *, delimiter:str = "\n", compressed:bool = False) -> DataGenerator:
        opener = gzip.open if compressed else open

        with opener(fofn, mode="rt") as f:
            last = ""

            while True:
                block = f.read(BLOCKSIZE)
                if not block:
                    break

                record_chunks = block.split(delimiter)
                if len(record_chunks) == 1:
                    last += record_chunks

                else:
                    yield Data(
                        filesystem = self,
                        address    = T.Path(last + record_chunks[0]))

                    for record in record_chunks[1:-1]:
                        yield Data(
                            filesystem = self,
                            address    = T.Path(record))

                    last = record_chunks[-1]

            if last:
                yield Data(
                    filesystem = self,
                    address    = T.Path(last))

    @property
    def supported_checksums(self) -> T.List[str]:
        return list(hashlib.algorithms_available)

    def _checksum(self, algorithm:str, address:T.Path) -> str:
        h = hashlib.new(algorithm)

        with open(address, "rb") as f:
            while True:
                block = f.read(BLOCKSIZE)
                if not block:
                    break

                h.update(block)

        return h.hexdigest()

    def _size(self, address:T.Path) -> int:
        return address.stat().st_size

    def delete_data(self, address:T.Path) -> None:
        address.unlink()
