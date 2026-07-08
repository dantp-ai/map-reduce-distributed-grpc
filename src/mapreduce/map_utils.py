from collections import defaultdict

import grpc
from google.protobuf.empty_pb2 import Empty

from mapreduce import config, logging_config, utils
from mapreduce.map_reduce_pb2 import Chunk
from mapreduce.map_reduce_pb2_grpc import DriverServiceStub

logger = logging_config.logger


def _emit_words(map_id: int, words, M: int) -> None:
    """Bucket ``words`` by first letter and append them to the per-bucket
    intermediate files ``mr-{map_id}-{bucket_id}`` under ``TMP_DIR_PATH``."""
    config.TMP_DIR_PATH.mkdir(exist_ok=True)

    file_handles = defaultdict(list)
    for word in words:
        bucket_id = ord(word[0]) % M
        intermediate_file = f"{config.TMP_DIR_PATH}/mr-{map_id}-{bucket_id}"
        file_handles[intermediate_file].append(f"{word}\n")

    for intermediate_file, word_buffer in file_handles.items():
        with open(intermediate_file, "a") as file_handle:
            file_handle.writelines(word_buffer)


def map_chunk(map_id: int, chunk: Chunk, M: int) -> None:
    """Process a single ``(path, start, end)`` byte-range chunk of a file.

    Reads exactly the chunk's bytes (seek-based, never the whole file),
    decodes, tokenizes, filters, and emits the words into intermediate buckets.
    Because chunk boundaries are whitespace-aligned, every word is wholly
    contained in exactly one chunk.
    """
    with open(chunk.path, "rb") as file:
        file.seek(chunk.start)
        raw = file.read(chunk.end - chunk.start)

    text = raw.decode(errors="ignore").lower()
    words = utils.filter_words(utils.tokenize(text))

    logger.info(f"[MAP] executing on chunk {chunk.path}[{chunk.start}:{chunk.end}]...")
    _emit_words(map_id, words, M)


def finish_map(address: str = config.SERVER_ADDRESS) -> None:
    with grpc.insecure_channel(address) as channel:
        stub = DriverServiceStub(channel)
        stub.FinishMap(Empty())


def map(map_id: int, chunks, M: int, address: str = config.SERVER_ADDRESS) -> None:
    logger.info(f"[MAP] running {map_id}...")
    for chunk in chunks:
        map_chunk(map_id, chunk, M)
    logger.info("Done.")
    finish_map(address)
