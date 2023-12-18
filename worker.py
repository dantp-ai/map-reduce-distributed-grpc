import logging
from contextlib import contextmanager
from enum import Enum

import grpc
from google.protobuf.empty_pb2 import Empty

import config
import logging_config
import map_utils
import reduce_utils
from map_reduce_pb2 import TaskInput
from map_reduce_pb2 import TaskType
from map_reduce_pb2_grpc import DriverServiceStub

logger = logging_config.logger


class WorkerState(Enum):
    Wait = 0
    Idle = 1
    Work = 2


class Worker:
    def __init__(self):
        self.state = WorkerState.Work

    def request_task(self) -> TaskInput:
        with grpc.insecure_channel(config.SERVER_ADDRESS) as channel:
            stub = DriverServiceStub(channel)
            task = stub.RequestTask(Empty())
        return task

    def run(self) -> None:
        while True:
            try:
                task = self.request_task()
                if task.type == TaskType.Map:
                    self.state = WorkerState.Work
                    map_utils.map(task.id, task.filePaths, task.M)
                elif task.type == TaskType.Reduce:
                    self.state = WorkerState.Work
                    reduce_utils.reduce(task.id)
                elif task.type == TaskType.NoTask:
                    if self.state != WorkerState.Idle:
                        logger.info("[WORKER] waiting for new available task.")
                    self.state = WorkerState.Idle
                else:
                    logger.info("[WORKER] shut down.")
                    return
            except grpc.RpcError:
                if self.state != WorkerState.Wait:
                    logging.info("[DRIVER] not started yet.")
                    self.state = WorkerState.Wait


@contextmanager
def profile_context():
    profiler = cProfile.Profile()
    profiler.enable()
    yield profiler
    profiler.disable()


if __name__ == "__main__":
    import pstats
    import cProfile
    import argparse
    import sys

    parser = argparse.ArgumentParser(description="Starts the worker.")
    parser.add_argument(
        "--name",
        dest="name",
        required=False if "--to_profile" not in sys.argv else True,
        type=str,
        help="Name for worker to differentiate profiling stats from other workers.",
    )
    parser.add_argument(
        "--profile", dest="to_profile", action="store_true", help="Enable the profiler"
    )
    args = parser.parse_args()

    if args.to_profile and not args.name:
        parser.error("--name argument is required when --profile is set.")

    worker = Worker()
    if args.to_profile:
        with profile_context() as pr:
            worker.run()
        stats = pstats.Stats(pr)
        stats.sort_stats(pstats.SortKey.TIME)
        filename_profiling = (
            f"./worker_{args.name}_profiling.prof"
            if args.name
            else "./worker_profiling.prof"
        )
        stats.dump_stats(filename=filename_profiling)
    else:
        worker.run()
