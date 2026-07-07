"""Regenerate the gRPC/protobuf stubs for the mapreduce package.

Run with:  uv run python scripts/generate_protos.py

grpc_tools.protoc emits a top-level ``import map_reduce_pb2`` in the generated
``*_pb2_grpc.py``; we rewrite it to a package-relative import so the stubs work
from inside the ``mapreduce`` package.
"""

import pathlib

import grpc_tools
from grpc_tools import protoc

ROOT = pathlib.Path(__file__).resolve().parents[1]
PKG = ROOT / "src" / "mapreduce"
# grpc_tools bundles the well-known protos (e.g. google/protobuf/empty.proto).
WELL_KNOWN = pathlib.Path(grpc_tools.__file__).parent / "_proto"


def main() -> None:
    exit_code = protoc.main(
        [
            "grpc_tools.protoc",
            f"-I{PKG}",
            f"-I{WELL_KNOWN}",
            f"--python_out={PKG}",
            f"--grpc_python_out={PKG}",
            str(PKG / "map_reduce.proto"),
        ]
    )
    if exit_code != 0:
        raise SystemExit(exit_code)

    grpc_stub = PKG / "map_reduce_pb2_grpc.py"
    text = grpc_stub.read_text()
    text = text.replace(
        "import map_reduce_pb2 as map__reduce__pb2",
        "from mapreduce import map_reduce_pb2 as map__reduce__pb2",
    )
    grpc_stub.write_text(text)
    print(f"Regenerated protobuf stubs in {PKG}")


if __name__ == "__main__":
    main()
