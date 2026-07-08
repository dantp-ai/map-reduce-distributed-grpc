import argparse
import cProfile
import glob
import os
import pstats
import time
from collections.abc import Iterator
from concurrent import futures
from contextlib import contextmanager
from threading import Event, Lock

import grpc
from google.protobuf.empty_pb2 import Empty

from mapreduce import config, logging_config, map_reduce_pb2_grpc
from mapreduce.map_reduce_pb2 import Chunk, TaskInput, TaskType

logger = logging_config.logger

# Whitespace bytes used to snap chunk boundaries so no word is ever split.
WHITESPACE_BYTES = b" \t\r\n"

# Default address the driver binds to (all interfaces, port 8000).
DEFAULT_BIND_ADDRESS = "[::]:8000"


@contextmanager
def profile_context():
    profiler = cProfile.Profile()
    profiler.enable()
    yield profiler
    profiler.disable()


def _next_boundary(path: str, target: int, size: int, block_size: int = 4096) -> int:
    """Return the index of the first whitespace byte at or after ``target``.

    Seeks to ``target`` and scans forward in small blocks until a whitespace
    byte is found, so a huge file is never read into memory. Returns ``size``
    if no whitespace is found before EOF (e.g. a single token longer than the
    chunk size still lands wholly in one chunk).
    """
    with open(path, "rb") as file:
        file.seek(target)
        offset = target
        while offset < size:
            block = file.read(block_size)
            if not block:
                break
            for i, byte in enumerate(block):
                if byte in WHITESPACE_BYTES:
                    return offset + i
            offset += len(block)
    return size


def split_file_into_chunks(path: str, chunk_size: int) -> Iterator[Chunk]:
    """Yield contiguous, word-boundary-aligned byte-range chunks of ``path``.

    Each chunk is roughly ``chunk_size`` bytes; every boundary is snapped to the
    next whitespace byte so a word is never split across chunks. An empty file
    yields no chunks.
    """
    size = os.path.getsize(path)
    start = 0
    while start < size:
        target = min(start + chunk_size, size)
        end = size if target >= size else _next_boundary(path, target, size)
        yield Chunk(path=path, start=start, end=end)
        start = end


def resolve_input_files(data_dir: str | None, input_file: str | None) -> list[str]:
    """Resolve the input ``.txt`` files from either a single file (``-file``) or
    a directory of ``*.txt`` files (``-dir``). ``-file`` takes precedence."""
    if input_file:
        return [input_file]
    return sorted(glob.glob(f"{data_dir}/*.txt"))


def assign_chunks_to_map_ids(
    N: int, files: list[str], chunk_size: int
) -> list[list[Chunk]]:
    """Split every input file into chunks, then round-robin them across N map
    ids. A map id may receive zero, one, or many chunks."""
    chunks = [
        chunk for path in files for chunk in split_file_into_chunks(path, chunk_size)
    ]
    return [chunks[i::N] for i in range(N)]


class DriverService(map_reduce_pb2_grpc.DriverServiceServicer):
    def __init__(
        self,
        N: int,
        M: int,
        files: list[str],
        chunk_size: int = config.DEFAULT_CHUNK_SIZE,
    ) -> None:
        self.N = N
        self.M = M
        self.files = files
        self.chunk_size = chunk_size
        self.state = TaskType.Map  # initially we start with Map
        self.task_id = 0
        self.done_count = 0
        self.event = Event()
        self.task_lock = Lock()
        self.chunks_to_map_id = assign_chunks_to_map_ids(N, files, chunk_size)

    def _get_map_task(self) -> TaskInput:
        map_id = self.task_id
        self.task_id += 1

        # All Map operations are taken
        if map_id == self.N - 1:
            self.state = TaskType.NoTask

        logger.info(f"[MAP] starting: {map_id}...")

        return TaskInput(
            type=TaskType.Map,
            id=map_id,
            chunks=self.chunks_to_map_id[map_id],
            M=self.M,
        )

    def _get_reduce_task(self) -> TaskInput:
        bucket_id = self.task_id
        self.task_id += 1

        # All Reduce tasks finished, no tasks available.
        if bucket_id == self.M - 1:
            self.state = TaskType.NoTask

        logger.info(f"[REDUCE] starting: {bucket_id}...")

        return TaskInput(type=TaskType.Reduce, id=bucket_id)

    def FinishMap(self, request: Empty, context: grpc.ServicerContext) -> Empty:
        with self.task_lock:
            self.done_count += 1
            logger.info("[MAP] task completed.")

            # Switch to Reduce phase as all Map tasks have finished
            if self.done_count == self.N:
                self.state = TaskType.Reduce
                self.task_id = 0
                self.done_count = 0
                logger.info("[MAP] Phase completed. Starting REDUCE phase...")
            return Empty()

    def FinishReduce(self, request: Empty, context: grpc.ServicerContext) -> Empty:
        with self.task_lock:
            self.done_count += 1
            logger.info("[REDUCE] task completed.")

            if self.done_count == self.M:
                logger.info("[MAP] [REDUCE] phase completed. Shutting down...")
                self.state = TaskType.ShutDown
                self.event.set()
            return Empty()

    def RequestTask(self, request: Empty, context: grpc.ServicerContext) -> TaskInput:
        with self.task_lock:
            if self.state == TaskType.Map:
                return self._get_map_task()
            if self.state == TaskType.Reduce:
                return self._get_reduce_task()
            return TaskInput(type=self.state)


def run(
    service: DriverService,
    num_workers: int = 4,
    address: str = DEFAULT_BIND_ADDRESS,
) -> None:
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=num_workers))
    map_reduce_pb2_grpc.add_DriverServiceServicer_to_server(service, server)
    server.add_insecure_port(address)

    logger.info(f"[DRIVER] starting on {address}...")
    server.start()
    service.event.wait()
    time.sleep(0.5)

    logger.info("[DRIVER] Stopping...")
    server.stop(0)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Starts the driver.")
    parser.add_argument("-N", dest="N", type=int, default=4, help="Number of MAP tasks")
    parser.add_argument(
        "-M", dest="M", type=int, default=6, help="Number of REDUCE tasks"
    )
    parser.add_argument(
        "-nw",
        dest="num_workers",
        type=int,
        default=4,
        help=(
            "The gRPC driver server uses a thread pool with a maximum of "
            "`num_workers` threads to handle incoming requests concurrently."
        ),
    )
    input_group = parser.add_mutually_exclusive_group()
    input_group.add_argument(
        "-dir",
        dest="data_dir",
        type=str,
        default="./data",
        help="Input directory; every *.txt file in it is processed (default: ./data).",
    )
    input_group.add_argument(
        "-file",
        dest="input_file",
        type=str,
        default=None,
        help="A single input .txt file to process instead of -dir.",
    )
    parser.add_argument(
        "--chunk-size",
        dest="chunk_size",
        type=int,
        default=config.DEFAULT_CHUNK_SIZE,
        help=(
            "Approximate size in bytes of each input chunk handed to a map "
            "task. Boundaries snap to the next whitespace so words are never "
            f"split (default: {config.DEFAULT_CHUNK_SIZE})."
        ),
    )
    parser.add_argument(
        "--address",
        dest="address",
        type=str,
        default=DEFAULT_BIND_ADDRESS,
        help="Address the driver binds to (default: [::]:8000).",
    )
    parser.add_argument(
        "--profile", dest="to_profile", action="store_true", help="Enable the profiler"
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    files = resolve_input_files(args.data_dir, args.input_file)
    service = DriverService(args.N, args.M, files, args.chunk_size)

    if args.to_profile:
        with profile_context() as pr:
            run(service, args.num_workers, args.address)
        stats = pstats.Stats(pr)
        stats.sort_stats(pstats.SortKey.TIME)
        stats.dump_stats(filename="./driver_profiling.prof")
    else:
        run(service, args.num_workers, args.address)


if __name__ == "__main__":
    main()
