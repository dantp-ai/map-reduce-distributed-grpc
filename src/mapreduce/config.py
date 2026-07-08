import os
from pathlib import Path

TMP_DIR_PATH = Path("./tmp")
OUT_DIR_PATH = Path("./out")

# Default address workers use to connect to the driver.
DEFAULT_ADDRESS = "localhost:8000"

# Environment variable that overrides the connect address.
ADDRESS_ENV_VAR = "MAPREDUCE_ADDRESS"


def resolve_address() -> str:
    """Return the driver connect address, honoring the MAPREDUCE_ADDRESS env var."""
    return os.environ.get(ADDRESS_ENV_VAR, DEFAULT_ADDRESS)


SERVER_ADDRESS = resolve_address()
