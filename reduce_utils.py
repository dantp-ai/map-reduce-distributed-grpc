import glob
from collections import Counter

import grpc
from google.protobuf.empty_pb2 import Empty

import config
import logging_config
from map_reduce_pb2_grpc import DriverServiceStub

logger = logging_config.logger


def reduce(reduce_id):
    logger.info(f"[REDUCE]: {reduce_id} executing...")
    files = glob.glob(f"{config.TMP_DIR_PATH}/*-{reduce_id}")
    counter = Counter()
    for file in files:
        with open(file, "r") as f:
            words = f.read().split()
        counter.update(words)

    config.OUT_DIR_PATH.mkdir(exist_ok=True)

    with open(f"{config.OUT_DIR_PATH}/out-{reduce_id}.txt", "w+") as file:
        file.write("\n".join(f"{word} {count}" for word, count in counter.items()))

    logger.info("Done.")
    finish_reduce()


def finish_reduce() -> None:
    with grpc.insecure_channel(config.SERVER_ADDRESS) as channel:
        stub = DriverServiceStub(channel)
        stub.FinishReduce(Empty())
