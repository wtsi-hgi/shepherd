
from pathlib import Path
from common.models.filesystems.types import Data
from lib.planning.transformers import strip_common_prefix, prefix, vault_transformer


test_case = [
    (
        Data(filesystem = "abc", address = Path("/path/to/my-project/.vault/.staged/01/23/45/67/89/ab-cGF0aC90by9zb21lL2ZpbGU=")),
        Data(filesystem ="xyz", address = Path("/path/to/my-project/.vault/.staged/01/23/45/67/89/ab-cGF0aC90by9zb21lL2ZpbGU="))
    ),

    (
        Data(filesystem = "abc", address = Path("/path/xyz/my-project/.vault/.staged/01/23/45/67/89/ab-cGF0aC90by9zb21lL2ZpbGU=")),
        Data(filesystem ="xyz", address = Path("/path/xyz/my-project/.vault/.staged/01/23/45/67/89/ab-cGF0aC90by9zb21lL2ZpbGU="))
    )

]


def run():
    transformed_io = vault_transformer(test_case)
    for source, target in transformed_io:
        print(f"Source: {source}, target: {target}")

if __name__ == "__main__":
    run()
