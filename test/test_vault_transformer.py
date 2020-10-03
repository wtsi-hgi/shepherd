
import unittest

from pathlib import Path
from common.models.filesystems.types import Data
from lib.planning.transformers import strip_common_prefix, prefix, vault_transformer

import grp
import pwd
import os



filepath_1 = Path("/lustre/scratch115/my-project/path/to/some/file")
vault_filepath_1 = Path("/lustre/scratch115/my-project/.vault/.staged/01/23/ab-cGF0aC90by9zb21lL2ZpbGU=")

filepath_2 = Path("/lustre/scratch116/my-project/path/to/other/file_other")
vault_filepath_2 = Path("/lustre/scratch116/my-project/.vault/.staged/56/78/cd-cGF0aC90by9vdGhlci9maWxlX290aGVy")


filepath_3 = Path ("/Users/pa11/Code/shepherd-testing/shepherd")
vault_filepath_3 = Path("/Users/pa11/Code/shepherd-testing/shepherd/.vault/.staged/56/78/cd-cGF0aC90by9vdGhlci9maWxlX290aGVy")



test_case = [

    (Data(filesystem = "abc", address = vault_filepath_3),
     Data(filesystem ="xyz", address = vault_filepath_3)),

    (Data(filesystem = "abc", address = vault_filepath_3),
     Data(filesystem ="xyz", address = vault_filepath_3))

]


class TestVaultTransformer(unittest.TestCase):

    def test_vault_transformer(self):
        transformed_io = vault_transformer(test_case)
        for source, target in transformed_io:
            print(f"source: {source.address}, target: {target.address}")
