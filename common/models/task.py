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

import subprocess
import stat
from dataclasses import dataclass
from tempfile import TemporaryDirectory

from .. import types as T
from .filesystems.types import Data


class ExitCode(T.Carrier[int]):
    """ Process exit code model """
    def __init__(self, exit_code:int) -> None:
        self.payload = exit_code

    @property
    def exit_code(self) -> int:
        # Convenience alias
        return self.payload

    def __bool__(self) -> bool:
        return self.payload == 0


@dataclass(frozen=True)
class Task:
    """ Task model """
    script:str
    source:Data
    target:Data

    def __call__(self) -> ExitCode:
        """ Execute the task """
        with TemporaryDirectory() as tmp:
            # Write task to disk and make it executable
            task = T.Path(tmp) / "task"
            task.touch(mode=stat.S_IRWXU)
            task.write_text(self.script)

            # Run the thing
            result = subprocess.run(str(task), cwd=tmp, check=False)
            return ExitCode(result.returncode)
