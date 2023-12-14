import collections
import glob
import shutil
import subprocess
from pathlib import Path

import config
import utils


def aggregate_word_counts(file_paths):
    word_counter = collections.Counter()

    for file_path in file_paths:
        with open(file_path, "r") as file:
            for line in file:
                parts = line.strip().split()
                word, count = parts[0], int(parts[1])
                word_counter[word] += count

    return word_counter


def naive_count_words():

    counter = collections.Counter()
    files = glob.glob(f"{Path(__name__).parents[0] / 'data'}/*.txt")

    for file in files:
        with open(file, "r") as file:
            text = file.read().lower()

        words = utils.tokenize(text)
        words = utils.filter_words(words)

        counter.update(words)

    return counter


def run_mapreduce_distributed(n_workers=4):
    cmd_worker = "python worker.py &"
    cmd_driver = "python driver.py"

    for _ in range(n_workers):
        with subprocess.Popen(cmd_worker, shell=True) as proc:
            proc.communicate(cmd_worker)
        assert proc.returncode == 0

    with subprocess.Popen(cmd_driver, shell=True) as proc2:
        proc2.communicate(cmd_driver)

    assert proc2.returncode == 0


def cleanup_directories():
    for dir_path in [config.OUT_DIR_PATH, config.TMP_DIR_PATH]:
        try:
            shutil.rmtree(dir_path)
            print(f"Deleted directory: {dir_path}")
        except Exception as e:
            print(f"Error deleting directory {dir_path}: {e}")


def test_mapreduce_end_to_end():
    run_mapreduce_distributed()
    gt_counts = naive_count_words()
    counts = aggregate_word_counts(file_paths=glob.glob(f"{config.OUT_DIR_PATH}/*.txt"))

    cleanup_directories()

    assert gt_counts == counts, "Ground truth counts != map-reduce counts."


if __name__ == "__main__":
    test_mapreduce_end_to_end()
