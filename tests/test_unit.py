from collections import Counter
from unittest import mock

import pytest
from google.protobuf.empty_pb2 import Empty

from mapreduce import config, driver, map_utils, reduce_utils, utils, worker
from mapreduce.driver import (
    DriverService,
    assign_chunks_to_map_ids,
    split_file_into_chunks,
)
from mapreduce.map_reduce_pb2 import Chunk, TaskType


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


# --- chunk splitting ------------------------------------------------------
# A crafted text whose boundaries fall mid-word, on spaces, and on newlines
# across the range of chunk sizes exercised below. Whitespace is limited to
# spaces and newlines, matching what utils.tokenize treats as separators.
SAMPLE_TEXT = (
    "the quick brown fox jumps over the lazy dog\n"
    "and the quick fox and the lazy dog become friends\n"
    "supercalifragilisticexpialidocious antidisestablishmentarianism\n"
    "a b c d e the the the\n"
)


def _expected_words(text: str) -> list[str]:
    return utils.filter_words(utils.tokenize(text.lower()))


@pytest.mark.parametrize("chunk_size", [1, 3, 7, 13, 16, 32, 64, 1000])
def test_split_file_never_breaks_a_word(tmp_path, chunk_size):
    src = tmp_path / "in.txt"
    src.write_bytes(SAMPLE_TEXT.encode())
    size = src.stat().st_size

    chunks = list(split_file_into_chunks(str(src), chunk_size))

    # Chunks are contiguous and cover the whole file exactly once.
    assert chunks[0].start == 0
    assert chunks[-1].end == size
    for prev, nxt in zip(chunks, chunks[1:], strict=False):
        assert prev.end == nxt.start

    # Every boundary sits at the file edge or on a whitespace byte, so no
    # word can straddle two chunks.
    raw = src.read_bytes()
    for chunk in chunks:
        for boundary in (chunk.start, chunk.end):
            assert boundary in (0, size) or raw[boundary] in b" \t\r\n"


def test_split_empty_file_yields_no_chunks(tmp_path):
    src = tmp_path / "empty.txt"
    src.write_bytes(b"")
    assert list(split_file_into_chunks(str(src), 8)) == []


def test_split_single_token_longer_than_chunk_size(tmp_path):
    # A token longer than chunk_size still lands wholly in one chunk.
    src = tmp_path / "one.txt"
    src.write_bytes(b"antidisestablishmentarianism")
    chunks = list(split_file_into_chunks(str(src), 4))
    assert len(chunks) == 1
    assert (chunks[0].start, chunks[0].end) == (0, src.stat().st_size)


# --- chunk correctness invariant (most important) -------------------------
@pytest.mark.parametrize("chunk_size", [1, 3, 7, 13, 16, 32, 64, 1000])
def test_chunks_preserve_word_multiset_end_to_end(tmp_path, monkeypatch, chunk_size):
    """For any chunk size, the multiset of words emitted across all chunks
    equals a naive count of the whole file: no word dropped or duplicated."""
    monkeypatch.setattr(config, "TMP_DIR_PATH", tmp_path / "tmp")
    src = tmp_path / "in.txt"
    src.write_bytes(SAMPLE_TEXT.encode())

    for chunk in split_file_into_chunks(str(src), chunk_size):
        map_utils.map_chunk(map_id=0, chunk=chunk, M=6)

    emitted = Counter()
    for path in (tmp_path / "tmp").glob("mr-0-*"):
        emitted.update(path.read_text().split())

    assert emitted == Counter(_expected_words(SAMPLE_TEXT))


# --- map bucketing --------------------------------------------------------
def test_map_chunk_buckets_words_by_first_letter(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "TMP_DIR_PATH", tmp_path / "tmp")
    src = tmp_path / "in.txt"
    src.write_bytes(b"Apple apple Bee\ncat")
    M = 2
    chunk = Chunk(path=str(src), start=0, end=src.stat().st_size)

    map_utils.map_chunk(map_id=0, chunk=chunk, M=M)

    buckets = {}
    for path in (tmp_path / "tmp").glob("mr-0-*"):
        bucket_id = int(path.name.split("-")[-1])
        buckets[bucket_id] = sorted(path.read_text().split())

    expected: dict[int, list[str]] = {}
    for word in ["apple", "apple", "bee", "cat"]:
        expected.setdefault(ord(word[0]) % M, []).append(word)
    expected = {k: sorted(v) for k, v in expected.items()}

    assert buckets == expected


# --- driver chunk assignment ----------------------------------------------
def test_assign_chunks_round_robin_covers_all_chunks(tmp_path):
    # One file split into many small chunks, spread across N=3 map tasks.
    (tmp_path / "in.txt").write_bytes(SAMPLE_TEXT.encode())

    all_chunks = list(split_file_into_chunks(str(tmp_path / "in.txt"), 8))
    groups = assign_chunks_to_map_ids(3, [str(tmp_path / "in.txt")], chunk_size=8)

    flat = [(c.path, c.start, c.end) for group in groups for c in group]
    expected = [(c.path, c.start, c.end) for c in all_chunks]
    # Every chunk appears exactly once across all map tasks.
    assert sorted(flat) == sorted(expected)
    assert len(flat) == len(expected)


def test_assign_chunks_fewer_than_tasks_leaves_empty_task_lists(tmp_path):
    # A tiny single-chunk input across N=4 tasks: 3 tasks get nothing.
    (tmp_path / "in.txt").write_bytes(b"hello world")
    groups = assign_chunks_to_map_ids(
        4, [str(tmp_path / "in.txt")], chunk_size=1_048_576
    )

    assert len(groups) == 4
    assert sum(len(g) for g in groups) == 1
    assert sum(1 for g in groups if not g) == 3


def test_assign_chunks_more_than_tasks(tmp_path):
    (tmp_path / "in.txt").write_bytes(SAMPLE_TEXT.encode())
    n_chunks = len(list(split_file_into_chunks(str(tmp_path / "in.txt"), 8)))
    assert n_chunks > 2  # sanity: crafted input really splits

    groups = assign_chunks_to_map_ids(2, [str(tmp_path / "in.txt")], chunk_size=8)
    assert sum(len(g) for g in groups) == n_chunks
    assert all(len(g) >= 1 for g in groups)


# --- input resolution: -file vs -dir --------------------------------------
def test_resolve_input_files_prefers_single_file():
    assert driver.resolve_input_files("./data", "/tmp/one.txt") == ["/tmp/one.txt"]


def test_resolve_input_files_globs_directory(tmp_path):
    (tmp_path / "b.txt").write_text("x")
    (tmp_path / "a.txt").write_text("x")
    assert driver.resolve_input_files(str(tmp_path), None) == [
        str(tmp_path / "a.txt"),
        str(tmp_path / "b.txt"),
    ]


def test_driver_parse_args_accepts_file():
    assert driver.parse_args(["-file", "corpus.txt"]).input_file == "corpus.txt"


def test_driver_rejects_both_file_and_dir():
    with pytest.raises(SystemExit):
        driver.parse_args(["-dir", "data", "-file", "corpus.txt"])


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
    service = DriverService(N=2, M=2, files=[])
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

    service = DriverService(N=1, M=1, files=[])
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
