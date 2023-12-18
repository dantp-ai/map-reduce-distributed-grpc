from collections import defaultdict

import grpc
from google.protobuf.empty_pb2 import Empty

import config
import logging_config
import utils
from map_reduce_pb2_grpc import DriverServiceStub

logger = logging_config.logger


def map_file(map_id: int, filename: str, M: int) -> None:

    config.TMP_DIR_PATH.mkdir(exist_ok=True)

    file_handles = defaultdict(list)

    with open(filename, "r") as file:
        text = file.read().lower()

        words = utils.tokenize(text)
        words = utils.filter_words(words)

    for word in words:
        bucket_id = ord(word[0]) % M
        intermediate_file = f"{config.TMP_DIR_PATH}/mr-{map_id}-{bucket_id}"

        file_handles[intermediate_file].append(f"{word}\n")

    logger.info(f"[MAP] executing on file {filename}...")
    for intermediate_file, word_buffer in file_handles.items():
        with open(intermediate_file, "a") as file_handle:
            file_handle.writelines(word_buffer)


def finish_map() -> None:
    with grpc.insecure_channel(config.SERVER_ADDRESS) as channel:
        stub = DriverServiceStub(channel)
        stub.FinishMap(Empty())


def map(map_id: int, file_paths, M: int) -> None:
    logger.info(f"[MAP] running {map_id}...")
    for fpath in file_paths:
        map_file(map_id, fpath, M)
    logger.info("Done.")
    finish_map()
