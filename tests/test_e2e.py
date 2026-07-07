"""End-to-end test: run the real driver and workers as subprocesses and check
that the distributed word counts match a naive single-process count."""

import collections
import glob
import shutil
import subprocess
import sys
from pathlib import Path

from mapreduce import utils

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
OUT_DIR = ROOT / "out"
TMP_DIR = ROOT / "tmp"


def naive_count_words(data_dir: Path) -> collections.Counter:
    counter = collections.Counter()
    for file in glob.glob(f"{data_dir}/*.txt"):
        text = Path(file).read_text().lower()
        counter.update(utils.filter_words(utils.tokenize(text)))
    return counter


def aggregate_word_counts(file_paths) -> collections.Counter:
    counter = collections.Counter()
    for path in file_paths:
        for line in Path(path).read_text().splitlines():
            if not line.strip():
                continue
            word, count = line.split()
            counter[word] += int(count)
    return counter


def cleanup() -> None:
    shutil.rmtree(OUT_DIR, ignore_errors=True)
    shutil.rmtree(TMP_DIR, ignore_errors=True)


def test_mapreduce_end_to_end():
    cleanup()
    N, M, num_workers = 4, 6, 4

    driver = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "mapreduce.driver",
            "-N",
            str(N),
            "-M",
            str(M),
            "-nw",
            str(num_workers),
            "-dir",
            str(DATA_DIR),
        ],
        cwd=ROOT,
    )
    workers = [
        subprocess.Popen([sys.executable, "-m", "mapreduce.worker"], cwd=ROOT)
        for _ in range(num_workers)
    ]

    try:
        driver.wait(timeout=60)
        for worker in workers:
            try:
                worker.wait(timeout=10)
            except subprocess.TimeoutExpired:
                worker.terminate()
    finally:
        for proc in [driver, *workers]:
            if proc.poll() is None:
                proc.kill()

    assert driver.returncode == 0, "driver did not exit cleanly"

    expected = naive_count_words(DATA_DIR)
    actual = aggregate_word_counts(glob.glob(f"{OUT_DIR}/*.txt"))
    cleanup()

    assert actual == expected, "map-reduce counts != naive counts"


if __name__ == "__main__":
    test_mapreduce_end_to_end()
