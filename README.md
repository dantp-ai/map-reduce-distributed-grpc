
### Setup

#### Using venv
0. Python: `2.7.15 | 3.6.7 | 3.7.1`
    * `grpcio-tools` requires either of these versions
1. Install virtualenv `python -m venv venv`
2. Activate virtualenv with `source venv/bin/activate`
3. `pip install -r requirements.txt`


#### Using miniconda
For me, it worked best with miniconda.

0. `brew install miniconda` (on macOS)
1. `conda env create -f environment.yaml`
2. `conda activate grpc-test`


> Assuming you are in the root directory of the repository, unless stated otherwise.

#### Running on example text files

0. If proto-generated files are not available, generate them like so:

    ```shell
    python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. map_reduce.proto
    ```

0. Open one terminal for the driver and open as many terminals as needed for each worker.

1. Start driver: `python driver.py -N 12 -M 8 -nw 4 -dir ./data`
    * `-N`: number of MAP tasks (default: 4)
    * `-M`: number of reduce tasks (default: 6)
    * `-nw`: number of maximum workers for gRPC driver server to process concurrently (default: 4)
    * `-dir`: Directory path to input data (default: `./data`)
    * `--profile`: Enable profiler (default: false)
2. Start worker in each window like so: `python worker.py`
    * `--name`: Name of the worker. Used to save different profiles for each worker.
    * `--profile`: Enable profiler (default: false)

* Final output of map-reduce can be found under `./out`.
  * There are as many files as reduce tasks. Each file contains word-count pairs separated by white-space.
* Intermediate output of map tasks can be found under `./tmp`.
  * There are `min(num_input_files*N, M*N)` intermediate files.
  * Each file contains words that fall in the bucket with `bucket_id = ord(first_character_word) % M`.

### Testing the example

* Run end-to-end test on the example files inside `data/`:

  ```shell
  python -m tests.test_e2e
  ```


### Notes

* Upon starting a worker it waits for driver to start and assign it a task (map, reduce or wait).
* If a worker has finished its MAP tasks and there are no other MAP tasks available, it waits until all other currently running MAP tasks have finished.
* When all tasks are done, driver shuts down and so do all the workers.
* Words from files are processed by:
  * making them lower-case
  * keeping only those words for which all their characters are in `a-z`
* Each MAP task is assigned to one or more text files using a cyclic order strategy.
  * The first task receives the first file, the second task receives the second file, and so on until the last task. If there are files remaining, the first task receives another file, and so on until all files have been assigned.

### Limitations

* A map task processes an entire input file. One could extend the implementation to allow chunks of the files for each map task.
* The implementation does not work with extremely large files.
    * Consider the case where one file is very large and all other relatively small. One worker would need a long time to finish while others are already done and waiting.
    * Reading the files in roughly equal chunks and creating map tasks for them is a solution.
        * Here, one should split appropriately such that the split does not occur in between a word, e.g. min(chunksize, position_of_last_blank)
