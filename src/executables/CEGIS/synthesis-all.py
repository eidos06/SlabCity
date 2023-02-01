import multiprocessing
import os
import sys
import time
from pathlib import Path
import logging
import subprocess

absolute_path = os.path.abspath(__file__)
sys.path.append(str(Path(absolute_path).parents[0]))
sys.path.append(str(Path(absolute_path).parents[1]))
sys.path.append(str(Path(absolute_path).parents[2]))
sys.path.append(str(Path(absolute_path).parents[3]))
from utils.csv_benchmark_getter import CsvBenchmarkGetter


def do_task(task):
    logging.info("running " + task["benchmark_id"] + ":" + task["query_id"])
    print("running " + task["benchmark_id"] + ":" + task["query_id"])
    try:
        subprocess.run([sys.executable,
                        "synthesis-single-benchmark.py",
                        task["benchmark_id"],
                        task["query_id"],
                        task["task_name"],
                        task["task_timestamp"],
                        task["log_folder"]
                        ], timeout=5)
    except Exception as err:
        logging.error("Exception:" + str(err))


def main(task_name, num_thread):
    timestamp = str(time.time())
    log_folder = f"../../../logs/{task_name}/{timestamp}"
    log_file = log_folder + "/overview.log"
    benchmark_folder = "../../../benchmarks/psql_filtered/equiv"
    schema_folder = "../../../benchmarks/schema"
    os.makedirs(log_folder, exist_ok=True)
    logging.basicConfig(filename=log_file, format='%(asctime)s %(levelname)-3s %(message)s',
                        datefmt='%m/%d/%Y %I:%M:%S')

    benchmark_reader = CsvBenchmarkGetter(benchmark_folder, schema_folder)

    task_list = []
    for benchmark in benchmark_reader.benchmarks:
        task_list.append({
            "benchmark_id": str(benchmark.problem_id),
            "query_id": str(benchmark.query_id),
            "task_name": task_name,
            "task_timestamp": timestamp,
            "log_folder": log_folder
        })
    print(f"total {len(task_list)} tasks generated")
    logging.info(f"total {len(task_list)} tasks generated")

    with multiprocessing.Pool(processes=num_thread) as pool:
        pool.map(do_task, task_list)
    print("finished all tasks")


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
