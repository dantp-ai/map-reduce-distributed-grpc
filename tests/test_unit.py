from pathlib import Path
from unittest import mock

from google.protobuf.empty_pb2 import Empty

from mapreduce import config, driver, map_utils, reduce_utils, utils, worker
from mapreduce.driver import DriverService, assign_files_to_map_ids
from mapreduce.map_reduce_pb2 import TaskType


# --- utils ----------------------------------------------------------------
def test_tokenize_splits_on_whitespace_and_newlines():
    assert utils.tokenize("the quick\nbrown fox") == ["the", "quick", "brown", "fox"]


def test_filter_words_keeps_only_lowercase_alpha():
    words = ["hello", "World", "abc123", "", "fox", "don't"]
    assert utils.filter_words(words) == ["hello", "fox"]


def test_tokenize_then_filter_drops_empty_tokens():
    tokens = utils.tokenize("a  b")
    assert "" in tokens
    assert utils.filter_words(tokens) == ["a", "b"]


# --- driver file assignment ----------------------------------------------
def test_assign_files_round_robin(tmp_path):
    for i in range(5):
        (tmp_path / f"f{i}.txt").write_text("x")

    groups = assign_files_to_map_ids(3, str(tmp_path))

    flat = [Path(f).name for group in groups for f in group]
    assert sorted(flat) == [f"f{i}.txt" for i in range(5)]
    # 5 files across 3 map tasks -> sizes 2, 2, 1
    assert sorted((len(g) for g in groups), reverse=True) == [2, 2, 1]


# --- map bucketing --------------------------------------------------------
def test_map_file_buckets_words_by_first_letter(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "TMP_DIR_PATH", tmp_path / "tmp")
    src = tmp_path / "in.txt"
    src.write_text("Apple apple Bee\ncat")
    M = 2

    map_utils.map_file(map_id=0, filename=str(src), M=M)

    buckets = {}
    for path in (tmp_path / "tmp").glob("mr-0-*"):
        bucket_id = int(path.name.split("-")[-1])
        buckets[bucket_id] = sorted(path.read_text().split())

    expected: dict[int, list[str]] = {}
    for word in ["apple", "apple", "bee", "cat"]:
        expected.setdefault(ord(word[0]) % M, []).append(word)
    expected = {k: sorted(v) for k, v in expected.items()}

    assert buckets == expected


# --- reduce aggregation ---------------------------------------------------
def test_reduce_aggregates_bucket_counts(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "TMP_DIR_PATH", tmp_path / "tmp")
    monkeypatch.setattr(config, "OUT_DIR_PATH", tmp_path / "out")
    monkeypatch.setattr(reduce_utils, "finish_reduce", lambda *args: None)
    (tmp_path / "tmp").mkdir()
    (tmp_path / "tmp" / "mr-0-3").write_text("the\nthe\nfox\n")
    (tmp_path / "tmp" / "mr-1-3").write_text("the\ndog\n")

    reduce_utils.reduce(3)

    out = (tmp_path / "out" / "out-3.txt").read_text()
    counts = dict(line.split() for line in out.splitlines())
    assert counts == {"the": "3", "fox": "1", "dog": "1"}


# --- driver state machine -------------------------------------------------
def test_driver_state_machine_full_cycle(tmp_path):
    service = DriverService(N=2, M=2, data_dir=str(tmp_path))
    ctx = None

    # MAP phase: exactly N map tasks are handed out, then NoTask.
    t0 = service.RequestTask(Empty(), ctx)
    assert (t0.type, t0.id) == (TaskType.Map, 0)
    t1 = service.RequestTask(Empty(), ctx)
    assert (t1.type, t1.id) == (TaskType.Map, 1)
    assert service.RequestTask(Empty(), ctx).type == TaskType.NoTask

    # Finishing all map tasks flips the driver into the REDUCE phase.
    service.FinishMap(Empty(), ctx)
    service.FinishMap(Empty(), ctx)

    r0 = service.RequestTask(Empty(), ctx)
    assert (r0.type, r0.id) == (TaskType.Reduce, 0)
    r1 = service.RequestTask(Empty(), ctx)
    assert (r1.type, r1.id) == (TaskType.Reduce, 1)
    assert service.RequestTask(Empty(), ctx).type == TaskType.NoTask

    # Finishing all reduce tasks shuts the driver down.
    service.FinishReduce(Empty(), ctx)
    service.FinishReduce(Empty(), ctx)
    assert service.state == TaskType.ShutDown
    assert service.event.is_set()


# --- configurable address -------------------------------------------------
def test_resolve_address_defaults_to_localhost(monkeypatch):
    monkeypatch.delenv(config.ADDRESS_ENV_VAR, raising=False)
    assert config.resolve_address() == "localhost:8000"


def test_resolve_address_honors_env_var(monkeypatch):
    monkeypatch.setenv(config.ADDRESS_ENV_VAR, "example.com:9999")
    assert config.resolve_address() == "example.com:9999"


def test_driver_parse_args_address_default():
    assert driver.parse_args([]).address == "[::]:8000"


def test_driver_parse_args_address_override():
    assert driver.parse_args(["--address", "[::]:9001"]).address == "[::]:9001"


def test_driver_run_binds_passed_address(monkeypatch):
    fake_server = mock.Mock()
    monkeypatch.setattr(driver.grpc, "server", lambda *a, **k: fake_server)
    monkeypatch.setattr(driver.time, "sleep", lambda *a, **k: None)

    service = DriverService(N=1, M=1, data_dir=".")
    service.event.set()  # so run() returns immediately without waiting

    driver.run(service, num_workers=1, address="[::]:12345")

    fake_server.add_insecure_port.assert_called_once_with("[::]:12345")


def test_worker_parse_args_address_default(monkeypatch):
    monkeypatch.delenv(config.ADDRESS_ENV_VAR, raising=False)
    assert worker.parse_args([]).address == "localhost:8000"


def test_worker_parse_args_address_falls_back_to_env(monkeypatch):
    monkeypatch.setenv(config.ADDRESS_ENV_VAR, "host:1234")
    assert worker.parse_args([]).address == "host:1234"


def test_worker_parse_args_address_override_beats_env(monkeypatch):
    monkeypatch.setenv(config.ADDRESS_ENV_VAR, "host:1234")
    assert worker.parse_args(["--address", "other:5678"]).address == "other:5678"
