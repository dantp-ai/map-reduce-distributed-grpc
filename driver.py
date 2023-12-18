import argparse
import glob
import time
from concurrent import futures
from contextlib import contextmanager
from threading import Event
from threading import Lock
from typing import List

import grpc
from google.protobuf.empty_pb2 import Empty

import logging_config
import map_reduce_pb2_grpc
from map_reduce_pb2 import TaskInput
from map_reduce_pb2 import TaskType

logger = logging_config.logger


@contextmanager
def profile_context():
    profiler = cProfile.Profile()
    profiler.enable()
    yield profiler
    profiler.disable()


def assign_files_to_map_ids(N: int, data_dir: str) -> List[List[str]]:
    files = glob.glob(f"{data_dir}/*.txt")

    # assign files to map IDs with round-robin strategy
    return [files[i::N] for i in range(N)]


class DriverService(map_reduce_pb2_grpc.DriverServiceServicer):
    def __init__(self, N: int, M: int, data_dir: str) -> None:
        self.N = N
        self.M = M
        self.data_dir = data_dir
        self.state = TaskType.Map  # initially we start with Map
        self.task_id = 0
        self.done_count = 0
        self.event = Event()
        self.task_lock = Lock()
        self.files_to_map_id = assign_files_to_map_ids(N, data_dir)

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
            filePaths=self.files_to_map_id[map_id],
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


def run(service: DriverService, num_workers: int = 4) -> None:
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=num_workers))
    map_reduce_pb2_grpc.add_DriverServiceServicer_to_server(service, server)
    server.add_insecure_port("[::]:8000")

    logger.info(f"[DRIVER] starting on {'[::]:8000'}...")
    server.start()
    service.event.wait()
    time.sleep(0.5)

    logger.info("[DRIVER] Stopping...")
    server.stop(0)


if __name__ == "__main__":

    import cProfile
    import pstats

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
        help="Specifies that the gRPC driver server should use a thread pool with a maximum of `num_workers` worker threads to handle incoming requests concurrently.",
    )
    parser.add_argument(
        "-dir", dest="data_dir", type=str, default="./data", help="Input data directory"
    )
    args = parser.parse_args()

    service = DriverService(args.N, args.M, args.data_dir)

    with profile_context() as pr:
        run(service, args.num_workers)

    stats = pstats.Stats(pr)
    stats.sort_stats(pstats.SortKey.TIME)
    stats.dump_stats(filename="./driver_profiling.prof")
