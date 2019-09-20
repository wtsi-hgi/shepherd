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

from common import types as T
from common.constants import BLOCKSIZE
from common.exceptions import NOT_IMPLEMENTED
from .transfer import FileGenerator, FilesystemVertex, UnsupportedByFilesystem


_NO_METADATA = UnsupportedByFilesystem("POSIX filesystems do not support key-value metadata")


class POSIXFilesystem(FilesystemVertex):
    """ Filesystem vertex implementation for POSIX-like filesystems """
    def _identify_by_metadata(self, **metadata:str) -> FileGenerator:
        raise _NO_METADATA

    def set_metadata(self, data:T.Path, **metadata:str) -> None:
        raise _NO_METADATA

    def delete_metadata(self, data:T.Path, *keys:str) -> None:
        raise _NO_METADATA

    def _accessible(self, data:T.Path) -> bool:
        return data.exists() and os.access(data, os.R_OK)

    def _identify_by_stat(self, path:T.Path, *, name:str = "*") -> FileGenerator:
        # TODO
        raise NOT_IMPLEMENTED

    def _identify_by_fofn(self, fofn:T.Path, *, delimiter:str = "\n", compressed:bool = False) -> FileGenerator:
        opener = open if not compressed else gzip.open

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
                    yield T.Path(last + record_chunks[0])

                    for record in record_chunks[1:-1]:
                        yield T.Path(record)

                    last = record_chunks[-1]

            yield T.Path(last)

    @property
    def supported_checksums(self) -> T.List[str]:
        return list(hashlib.algorithms_available)

    def _checksum(self, algorithm:str, data:T.Path) -> str:
        h = hashlib.new(algorithm)

        with open(data, "rb") as f:
            while True:
                block = f.read(BLOCKSIZE)
                if not block:
                    break

                h.update(block)

        return h.hexdigest()

    def delete_data(self, data:T.Path) -> None:
        data.unlink()
