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

# Exception Hierarchy:
#
# Exception
#  |
#  +-- StateException
#       |
#       +-- BackendException
#       |    |
#       |    +-- LogicException
#       |    |
#       |    +-- NoFilesystemConvertor
#       |
#       +-- DataException
#            |
#            +-- DataNotReady
#            |    |
#            |    +-- PeriodNotStarted
#            |    |
#            |    +-- NoThroughputData
#            |    |
#            |    +-- NoTasksAvailable
#            |
#            +-- WorkerRedundant
#            |
#            +-- NoCommonChecksumAlgorithm

class StateException(Exception):
    """ Base state engine exception """

class BackendException(StateException):
    """ Base engine exception """

class LogicException(BackendException):
    """ Raised when the backend raises a logic error (as opposed to a data error) """

class NoFilesystemConvertor(BackendException):
    """ Raised when data cannot be mapped to a filesystem instance """

class DataException(StateException):
    """ Base state exception """

class DataNotReady(DataException):
    """ Raised when data is not yet available """

class PeriodNotStarted(DataNotReady):
    """ Raised when a temporal period is queried but hasn't started """

class NoThroughputData(DataNotReady):
    """ Raised when no relevant throughput data is available """

class NoTasksAvailable(DataNotReady):
    """ Raised when no tasks are currently available to attempt """

class WorkerRedundant(DataException):
    """ Raised when a worker has nothing to do """
    # FIXME Does this belong here?

class NoCommonChecksumAlgorithm(DataException):
    """ Raised when filesystems do not share a common checksumming algorithm """
    # FIXME Does this belong here?
