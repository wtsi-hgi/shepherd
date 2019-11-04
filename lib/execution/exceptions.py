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

# Exception Hierarchy:
#
# Exception
#  |
#  +-- ExecutionException
#       |
#       +-- SubmissionException
#       |    |
#       |    +-- CouldNotSubmit
#       |
#       +-- WorkerException
#            |
#            +-- NoSuchWorker
#            |
#            +-- CouldNotAddressWorker
#            |
#            +-- NotAWorker

class ExecutionException(Exception):
    """ Base execution exception """

class SubmissionException(ExecutionException):
    """ Base submission exception """

class CouldNotSubmit(SubmissionException):
    """ Raised when a submission failed """

class WorkerException(ExecutionException):
    """ Base worker exception """

class NoSuchWorker(WorkerException):
    """ Raised when a worker cannot be dereferenced """

class CouldNotAddressWorker(WorkerException):
    """ Raised when a worker cannot be addressed """

class NotAWorker(WorkerException):
    """ Raised when worker-specific invocations are made against non-workers """
