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
from common.models.filesystems import POSIXFilesystem, iRODSFilesystem
from .templating import transfer_script
from .types import FilesystemVertex, TransferRoute, PolynomialComplexity, On


# TODO This module probably won't be necessary in the end because routes
# will be defined in software or by configuration


_posix_to_irods = T.Path("lib/planning/templates/posix_to_irods.sh.j2")
_script = transfer_script(_posix_to_irods.read_text())

def posix_to_irods_factory(posix:POSIXFilesystem, irods:iRODSFilesystem, *, cost:PolynomialComplexity = On) -> TransferRoute:
    """ Create POSIX to iRODS route """
    return TransferRoute(FilesystemVertex(posix), FilesystemVertex(irods), templating=_script, cost=cost)
