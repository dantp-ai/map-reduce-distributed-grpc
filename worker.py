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
    import sys

    name = sys.argv[1]

    worker = Worker()
    with profile_context() as pr:
        worker.run()

    stats = pstats.Stats(pr)
    stats.sort_stats(pstats.SortKey.TIME)
    stats.dump_stats(filename=f"./worker_{name}_profiling.prof")
