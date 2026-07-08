"""Unit tests for the worker loop: retry backoff and clean shutdown.

`time.sleep` is patched everywhere so the tests run instantly.
"""

import grpc
import pytest

from mapreduce import worker as worker_mod
from mapreduce.map_reduce_pb2 import Chunk, TaskInput, TaskType
from mapreduce.worker import Worker


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    monkeypatch.setattr(worker_mod.time, "sleep", lambda *_args, **_kwargs: None)


def test_run_exits_after_bounded_failures(monkeypatch):
    """When the driver is never reachable, run() returns after exactly
    `max_failures` consecutive RpcErrors instead of looping forever."""
    calls = {"n": 0}

    def always_fail(self):
        calls["n"] += 1
        raise grpc.RpcError()

    monkeypatch.setattr(Worker, "request_task", always_fail)

    Worker(max_failures=3).run()

    assert calls["n"] == 3


def test_run_dispatches_map_then_shuts_down(monkeypatch):
    """A Map task is dispatched, then a ShutDown task ends the loop."""
    chunk = Chunk(path="f.txt", start=0, end=10)
    tasks = iter(
        [
            TaskInput(type=TaskType.Map, id=0, chunks=[chunk], M=2),
            TaskInput(type=TaskType.ShutDown),
        ]
    )
    monkeypatch.setattr(Worker, "request_task", lambda self: next(tasks))

    mapped = []
    monkeypatch.setattr(
        worker_mod.map_utils,
        "map",
        lambda map_id, chunks, M, address: mapped.append(
            (map_id, [(c.path, c.start, c.end) for c in chunks], M)
        ),
    )

    Worker().run()

    assert mapped == [(0, [("f.txt", 0, 10)], 2)]


def test_failure_counter_resets_after_success(monkeypatch):
    """A successful request between failures resets the consecutive-failure
    counter, so the worker does not exit prematurely."""
    outcomes = [
        "raise",
        "raise",
        TaskInput(type=TaskType.NoTask),
        "raise",
        TaskInput(type=TaskType.ShutDown),
    ]
    calls = {"n": 0}

    def scripted_request(self):
        item = outcomes[calls["n"]]
        calls["n"] += 1
        if item == "raise":
            raise grpc.RpcError()
        return item

    monkeypatch.setattr(Worker, "request_task", scripted_request)

    Worker(max_failures=3).run()

    # All five outcomes are consumed: the NoTask success reset the counter so
    # the two failures on either side never accumulated to the bound.
    assert calls["n"] == 5
