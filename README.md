# Distributed MapReduce over gRPC

A small distributed MapReduce that counts words across text files.
A single driver coordinates a pool of workers over gRPC: the driver hands out map and reduce tasks, and the workers execute them.

![MapReduce walkthrough](docs/output.gif)

> The animation is generated from [`docs/visualization.html`](docs/visualization.html); open that file in a browser for the interactive version.
> It illustrates a single file split into chunks.

## Requirements

- Python 3.12+.
- [uv](https://docs.astral.sh/uv/) for environment and dependency management.

## Setup

```shell
uv sync
```

This also installs the project itself, which exposes the `mapreduce-driver` and `mapreduce-worker` commands.

## Running

Run the example on the text files in `data/`.
Open one terminal for the driver and one terminal per worker.

Start the driver:

```shell
uv run mapreduce-driver -N 12 -M 8 -nw 4 -dir ./data
```

- `-N`: number of MAP tasks (default: 4).
- `-M`: number of REDUCE tasks (default: 6).
- `-nw`: maximum number of worker threads the gRPC driver server uses to handle requests concurrently (default: 4).
- `-dir`: directory containing the input `.txt` files (default: `./data`).
- `-file`: a single input `.txt` file to process instead of `-dir`.
  `-dir` and `-file` are mutually exclusive - pass at most one.
- `--chunk-size`: approximate size in bytes of each input chunk handed to a map task (default: `1048576`, i.e. 1 MiB).
  Input files are split into contiguous chunks of roughly this size; each boundary snaps to the next whitespace so words are never split.
- `--address`: address the driver binds to (default: `[::]:8000`).
- `--profile`: enable the profiler (default: off).

To process a single file instead of a whole directory, use `-file`:

```shell
uv run mapreduce-driver -file ./data/big.txt --chunk-size 65536
```

Start each worker in its own terminal:

```shell
uv run mapreduce-worker
```

- `--name`: worker name, used to label its profiling output (required when `--profile` is set).
- `--address`: address of the driver to connect to.
  Defaults to the `MAPREDUCE_ADDRESS` environment variable, or `localhost:8000` if that is unset.
- `--profile`: enable the profiler (default: off).

To run the driver and workers on a non-default port, point them at the same address:

```shell
uv run mapreduce-driver --address "[::]:9000"
uv run mapreduce-worker --address localhost:9000
```

Alternatively, set the `MAPREDUCE_ADDRESS` environment variable so every worker picks it up without passing `--address`:

```shell
export MAPREDUCE_ADDRESS=localhost:9000
uv run mapreduce-worker
```

Outputs:

- Final output is written to `./out`, one file per reduce task, each containing word-count pairs separated by whitespace.
- Intermediate map output is written to `./tmp`.
  Each intermediate file holds the words whose bucket is `bucket_id = ord(first_character_of_word) % M`.

## Testing

```shell
uv run pytest
```

The suite has unit tests (tokenizing, filtering, chunk splitting, chunk assignment, bucketing, reducing, and the driver state machine).
A key invariant test checks that, across many chunk sizes, the multiset of words emitted from all chunks of a file equals a naive single-process count of the whole file: no word is dropped or duplicated.
It also has one end-to-end test that runs the real driver and workers as subprocesses (with a small chunk size so the sample files split) and compares the result against a naive single-process word count.

## Regenerating the protobuf stubs

The generated stubs (`src/mapreduce/map_reduce_pb2*.py`) are committed to the repository.
Regenerate them after editing `src/mapreduce/map_reduce.proto`:

```shell
uv run python scripts/generate_protos.py
```

## How it works

- On startup, each worker waits for the driver and asks it for a task (map, reduce, or wait).
- A worker that finishes its map tasks waits until all other in-flight map tasks are done before the reduce phase starts.
- When all tasks are done, the driver shuts down and the workers exit.
- Words are normalized by lowercasing the text and keeping only words whose characters are all in `a-z`.
- Input files are split into contiguous byte-range chunks of roughly `--chunk-size` bytes.
  Each chunk boundary is snapped forward to the next whitespace byte, so every word is wholly contained in exactly one chunk (no word is ever split).
  The splitter seeks to each candidate boundary instead of scanning the whole file, so very large files are handled without reading them into memory.
- The chunks are then assigned to the `-N` map tasks with a round-robin strategy: the first chunk goes to the first task, the second chunk to the second task, and so on, wrapping around until every chunk is assigned.
  A map task may receive zero, one, or many chunks; a task with no chunks still completes so the phase can advance.

## Limitations

Input files are now split into word-boundary chunks that are spread across the map tasks, so a map task processes chunks of a file rather than whole files, and a single large file is divided into many chunks instead of pinning one worker.
Two residual caveats remain:

- A single token longer than `--chunk-size` (a "word" with no whitespace inside it) still lands wholly in one chunk, so that one chunk can exceed the target size.
  This keeps every word intact and is a non-issue for natural-language text.
- Chunking reduces load skew but does not eliminate it.
  Work is balanced by number of chunks, not by the number of words a chunk contains, and chunks are assigned statically up front rather than pulled on demand, so some imbalance can remain.
