import argparse
import cProfile
import pstats
import time
from contextlib import contextmanager
from enum import Enum

import grpc
from google.protobuf.empty_pb2 import Empty

from mapreduce import config, logging_config, map_utils, reduce_utils
from mapreduce.map_reduce_pb2 import TaskInput, TaskType
from mapreduce.map_reduce_pb2_grpc import DriverServiceStub

logger = logging_config.logger


class WorkerState(Enum):
    Wait = 0
    Idle = 1
    Work = 2


class Worker:
    def __init__(
        self,
        address: str = config.SERVER_ADDRESS,
        max_failures: int = 10,
        base_backoff: float = 0.2,
        max_backoff: float = 1.0,
    ) -> None:
        self.state = WorkerState.Work
        self.address = address
        self.max_failures = max_failures
        self.base_backoff = base_backoff
        self.max_backoff = max_backoff

    def request_task(self) -> TaskInput:
        with grpc.insecure_channel(self.address) as channel:
            stub = DriverServiceStub(channel)
            task = stub.RequestTask(Empty())
        return task

    def run(self) -> None:
        failures = 0
        while True:
            try:
                task = self.request_task()
                # A reachable driver resets the consecutive-failure counter,
                # so an early startup wait never counts against the bound.
                failures = 0
                if task.type == TaskType.Map:
                    self.state = WorkerState.Work
                    map_utils.map(task.id, task.filePaths, task.M, self.address)
                elif task.type == TaskType.Reduce:
                    self.state = WorkerState.Work
                    reduce_utils.reduce(task.id, self.address)
                elif task.type == TaskType.NoTask:
                    if self.state != WorkerState.Idle:
                        logger.info("[WORKER] waiting for new available task.")
                    self.state = WorkerState.Idle
                else:
                    logger.info("[WORKER] shut down.")
                    return
            except grpc.RpcError:
                failures += 1
                if failures >= self.max_failures:
                    logger.info(
                        f"[WORKER] driver unreachable after {failures} "
                        "attempts; shutting down."
                    )
                    return
                if self.state != WorkerState.Wait:
                    logger.info("[DRIVER] not started yet.")
                    self.state = WorkerState.Wait
                # Back off (capped exponential) so we do not busy-spin
                # while the driver is down.
                delay = min(self.base_backoff * 2 ** (failures - 1), self.max_backoff)
                time.sleep(delay)


@contextmanager
def profile_context():
    profiler = cProfile.Profile()
    profiler.enable()
    yield profiler
    profiler.disable()


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Starts the worker.")
    parser.add_argument(
        "--name",
        dest="name",
        type=str,
        help="Name for the worker, used to label its profiling stats.",
    )
    parser.add_argument(
        "--address",
        dest="address",
        type=str,
        default=config.resolve_address(),
        help=(
            "Address of the driver to connect to. Falls back to the "
            "MAPREDUCE_ADDRESS env var, then localhost:8000."
        ),
    )
    parser.add_argument(
        "--profile", dest="to_profile", action="store_true", help="Enable the profiler"
    )
    args = parser.parse_args(argv)
    if args.to_profile and not args.name:
        parser.error("--name is required when --profile is set.")
    return args


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    worker = Worker(args.address)
    if args.to_profile:
        with profile_context() as pr:
            worker.run()
        stats = pstats.Stats(pr)
        stats.sort_stats(pstats.SortKey.TIME)
        stats.dump_stats(filename=f"./worker_{args.name}_profiling.prof")
    else:
        worker.run()


if __name__ == "__main__":
    main()
