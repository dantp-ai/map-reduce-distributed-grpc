import threading

import grpc
from google.protobuf.empty_pb2 import Empty

import config
import logging_config
import utils
from map_reduce_pb2_grpc import DriverServiceStub

logger = logging_config.logger


def map_file(map_id: int, filename: str, M: int) -> None:

    tmp_path_lock = threading.Lock()
    with tmp_path_lock:
        if not config.TMP_DIR_PATH.exists():
            config.TMP_DIR_PATH.mkdir(exist_ok=True)

    logger.info(f"[MAP] on file {filename}")
    with open(filename, "r") as file:
        text = file.read().lower()

        words = utils.tokenize(text)
        words = utils.filter_words(words)

        for word in words:
            bucket_id = ord(word[0]) % M
            intermediate_file = f"{config.TMP_DIR_PATH}/mr-{map_id}-{bucket_id}"
            with open(intermediate_file, "a") as bf:
                bf.write(f"{word}\n")


def finish_map() -> None:
    with grpc.insecure_channel(config.SERVER_ADDRESS) as channel:
        stub = DriverServiceStub(channel)
        stub.FinishMap(Empty())


def map(map_id: int, file_paths, M: int) -> None:
    logger.info(f"[MAP] running {map_id}...")
    for fpath in file_paths:
        map_file(map_id, fpath, M)
    finish_map()
