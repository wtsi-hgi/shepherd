
from pathlib import Path
from common.models.filesystems.types import Data
from lib.planning.transformers import strip_common_prefix, prefix, vault_transformer




filepath_1 = Path("/lustre/scratch115/crook-test/path/to/some/file")
vault_filepath_1 = Path("/lustre/scratch115/crook-test/.vault/.staged/01/23/ab-cGF0aC90by9zb21lL2ZpbGU=")

filepath_2 = Path("/lustre/scratch115/crook-test/path/to/other/file_other")
vault_filepath_2 = Path("/lustre/scratch115/crook-test/.vault/.staged/56/78/cd-cGF0aC90by9vdGhlci9maWxlX290aGVy")



test_case = [
    (
        Data(filesystem = "abc", address = vault_filepath_1),
        Data(filesystem ="xyz", address = vault_filepath_1)
    ),

    (
        Data(filesystem = "abc", address = vault_filepath_2),
        Data(filesystem ="xyz", address = vault_filepath_2)
    )

]


def run():
    transformed_io = vault_transformer(test_case)
    for source, target in transformed_io:
        print(f"Source: {source}, target: {target}")

if __name__ == "__main__":
    run()
    # import base64
    # path = "path/to/other/file_other"
    # encoded = base64.b64encode(b"path/to/some/file")
    # print(encoded)

