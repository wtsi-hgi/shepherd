"""
Copyright (c) 2020 Genome Research Limited

Authors:
* Christopher Harrison <ch12@sanger.ac.uk>
* Piyush Ahuja <pa11@sanger.ac.uk>

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

import unittest
from unittest.mock import MagicMock, patch

from common import types as T
from common.models.filesystems.types import Data
from lib.planning.types import IOGenerator
from lib.planning.transformers.vault import _vault_transformer


# Helper functions
def _make_case(source:str, group:str, target:str) -> T.Tuple[T.Path, str, T.Path]:
    return T.Path(source), group, T.Path(target)

def _make_data(address:T.Path) -> T.Tuple[Data, Data]:
    data = Data(filesystem=MagicMock(), address=address)
    return data, data


_CASES = [
    # Projects
    _make_case("/lustre/scratch101/projects/my_project/.vault/.staged/01/23/45/67/89/ab-Zm9vL2Jhci9xdXV4",
               "my_project", "/humgen/projects/my_project/scratch101/foo/bar/quux"),
    _make_case("/lustre/scratch101/realdata/mdt0/projects/my_project/.vault/.staged/01/23/45/67/89/ab-Zm9vL2Jhci9xdXV4",
               "my_project", "/humgen/projects/my_project/scratch101/foo/bar/quux"),

    # Teams
    _make_case("/lustre/scratch101/teams/my_team/.vault/.staged/01/23/45/67/89/ab-Zm9vL2Jhci9xdXV4",
               "my_team", "/humgen/teams/my_team/scratch101/foo/bar/quux"),
    _make_case("/lustre/scratch101/realdata/mdt0/teams/my_team/.vault/.staged/01/23/45/67/89/ab-Zm9vL2Jhci9xdXV4",
               "my_team", "/humgen/teams/my_team/scratch101/foo/bar/quux"),

    # HGI Team (special case)
    _make_case("/lustre/scratch101/.vault/.staged/01/23/45/67/89/ab-Zm9vL2Jhci9xdXV4",
               "hgi", "/humgen/teams/hgi/scratch101/foo/bar/quux"),
    _make_case("/lustre/scratch101/realdata/mdt0/.vault/.staged/01/23/45/67/89/ab-Zm9vL2Jhci9xdXV4",
               "hgi", "/humgen/teams/hgi/scratch101/foo/bar/quux")
]

_EXPECTED = (expected for _, _, expected in _CASES)

class TestVaultTransformer(unittest.TestCase):
    def test_transformation(self):
        def io() -> IOGenerator:
            # IO generator that iterates through our test cases and sets
            # the mock group appropriately for the downstream transformer
            for source, group, _ in _CASES:
                with patch("pathlib.Path.group") as mock_group:
                    mock_group.return_value = group
                    yield _make_data(source)

        for (_, target), expected in zip(_vault_transformer(io()), _EXPECTED):
            self.assertEqual(target.address, expected)

    def test_failure(self):
        io = [_make_data(T.Path("/not/a/vault/path"))]
        transformed = list(_vault_transformer(io))
        self.assertEqual(len(transformed), 0)


if __name__ == "__main__":
    unittest.main()
