from collections import defaultdict

import grpc
from google.protobuf.empty_pb2 import Empty

from mapreduce import config, logging_config, utils
from mapreduce.map_reduce_pb2_grpc import DriverServiceStub

logger = logging_config.logger


def map_file(map_id: int, filename: str, M: int) -> None:

    config.TMP_DIR_PATH.mkdir(exist_ok=True)

    file_handles = defaultdict(list)

    def _process_word(word):
        bucket_id = ord(word[0]) % M
        intermediate_file = f"{config.TMP_DIR_PATH}/mr-{map_id}-{bucket_id}"
        file_handles[intermediate_file].append(f"{word}\n")

    with open(filename) as file:
        text = file.read().lower()

        words = utils.tokenize(text)
        words = utils.filter_words(words)

    [_process_word(word) for word in words]

    logger.info(f"[MAP] executing on file {filename}...")
    for intermediate_file, word_buffer in file_handles.items():
        with open(intermediate_file, "a") as file_handle:
            file_handle.writelines(word_buffer)


def finish_map(address: str = config.SERVER_ADDRESS) -> None:
    with grpc.insecure_channel(address) as channel:
        stub = DriverServiceStub(channel)
        stub.FinishMap(Empty())


def map(map_id: int, file_paths, M: int, address: str = config.SERVER_ADDRESS) -> None:
    logger.info(f"[MAP] running {map_id}...")
    for fpath in file_paths:
        map_file(map_id, fpath, M)
    logger.info("Done.")
    finish_map(address)
