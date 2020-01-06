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
from common.models.api import API, RequiredArgument, OptionalArgument

# API endpoints
from common.models.filesystems import POSIXFilesystem, iRODSFilesystem
from lib.execution.lsf import LSF
from lib.planning import transformers as transformer


# NOTE The API class is a Callable and used to wrap callables, such as
# functions and classes, in order to expose them -- with argument
# definitions and help -- to userland. However, if you wish to refer to
# the callable directly (i.e., rather than calling it), then use its
# callable attribute.

# FIXME I'm not convinced that this is a good idea...marking as
# DEPRECATED for now


_Registrations = T.Dict[str, API]

filesystems:_Registrations = {
    "POSIXFilesystem": API(
        callable=POSIXFilesystem,
        arguments=[
            RequiredArgument("name", str, default="POSIX", help="Name for the filesystem"),
            RequiredArgument("max_concurrency", int, default=1, help="Maximum concurrency")
        ],
        help="POSIX-like filesystem driver"
    ),
    "iRODSFilesystem": API(
        callable=iRODSFilesystem,
        arguments=[
            RequiredArgument("name", str, default="iRODS", help="Name for the filesystem"),
            RequiredArgument("max_concurrency", int, default=10, help="Maximum concurrency")
        ],
        help="iRODS filesystem driver"
    )
}

executors:_Registrations = {
    "LSF": API(
        callable=LSF,
        arguments=[
            RequiredArgument("name", str, default="LSF", help="Name for the executor"),
            RequiredArgument("config_dir", T.Path, help="Directory of the LSF cluster configuration")
        ],
        help="LSF executor"
    )
}

transformers:_Registrations = {
    "prefix": API(
        callable=transformer.prefix,
        arguments=[
            RequiredArgument("prefix", T.Path, help="Path to prefix to target files")
        ],
        help="Target path prefixion"
    ),
    "strip_common_prefix": API(
        callable=transformer.strip_common_prefix,
        help="Strip the common path prefix from target files"
    ),
    "last_n_components": API(
        callable=transformer.last_n_components,
        arguments=[
            RequiredArgument("n", int, help="Maximum number of components to keep from the end")
        ],
        help="Take path components from the end of the target files"
    ),
    "telemetry": API(
        callable=transformer.telemetry,
        help="Add telemetry information into the output of the transfer process"
    ),
    "debugging": API(
        callable=transformer.debugging,
        help="Add shell debugging to the transfer process"
    )
}
