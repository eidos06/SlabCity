import os
import sys
from pathlib import Path
absolute_path = os.path.abspath(__file__)
sys.path.append(str(Path(absolute_path).parents[0]))
sys.path.append(str(Path(absolute_path).parents[1]))
sys.path.append(str(Path(absolute_path).parents[2]))
sys.path.append(str(Path(absolute_path).parents[3]))
from utils.csv_benchmark_getter import CsvBenchmarkGetter
from lib.optimize import optimize
from pglast.parser import parse_sql
import time
import csv
import logging
import traceback
from testing.oracle import Oracle
import yaml
import uuid

benchmark_id = str(sys.argv[1])
query_id = str(sys.argv[2])
task_name = str(sys.argv[3])
task_timestamp = str(sys.argv[4])
log_folder = str(sys.argv[5])

benchmark_folder = "../../../benchmarks/psql_filtered/equiv"
schema_folder = "../../../benchmarks/schema"
result_stored_base = "../../../experiment_result"
result_stored = result_stored_base + f"/{task_name}/{task_timestamp}/queries/{benchmark_id}/{query_id}.txt"
status_stored = result_stored_base + f"/{task_name}/{task_timestamp}/syn-status/{benchmark_id}/{query_id}.csv"
breaks_stored = result_stored_base + f"/{task_name}/{task_timestamp}/break-status/{benchmark_id}/{query_id}.bool"
log_file = log_folder + f"/{benchmark_id}-{query_id}.log"

os.makedirs(os.path.dirname(result_stored), exist_ok=True)
os.makedirs(os.path.dirname(log_file), exist_ok=True)
os.makedirs(os.path.dirname(status_stored), exist_ok=True)
os.makedirs(os.path.dirname(breaks_stored), exist_ok=True)
logging.basicConfig(filename=log_file, format='%(asctime)s %(levelname)-3s %(message)s', datefmt='%m/%d/%Y %I:%M:%S')

benchmark_getter = CsvBenchmarkGetter(benchmark_folder, schema_folder)

# set up oracle
oracle_config_file = "../../testing/config.yml"
with open(oracle_config_file) as oracle_cf:
    oracle_config = yaml.safe_load(oracle_cf)
    oracle_config["tester"]["psql"]["dbname"] = "test" + str(uuid.uuid4().hex)
    oracle_config["problem"] = benchmark_id
oracle = Oracle(oracle_config, query_id)

try:
    query = benchmark_getter.get_query_text_by_pid_and_qid(benchmark_id, query_id)
    query = parse_sql(query)[0].stmt
    schema = benchmark_getter.get_schema_by_benchmark_name(benchmark_id)

    start_time = time.perf_counter()

    with open(result_stored, "w") as f:
        with open(status_stored, "w") as status_csv:
            writer_query = csv.writer(f)
            writer_status = csv.writer(status_csv)
            writer_status.writerow(["num_total_generated", "num_filter_after_oracle", "num_oracle_break", "total_synthesis_time", "total_oracle_test_time"])
            for candidate, num_total_generated, num_filtered_after_oracle, num_oracle_break, total_synthesis_time, total_oracle_test_time in optimize(query, schema, oracle):
                writer_query.writerow([str(candidate)])
                f.flush()
                writer_status.writerow([num_total_generated, num_filtered_after_oracle, num_oracle_break, total_synthesis_time, total_oracle_test_time])
                status_csv.flush()
                if num_oracle_break != 0:
                    with open(breaks_stored, "w") as tmp:
                        tmp.write("!")
                        tmp.flush()
except Exception as e:
    logging.warning(e)
    logging.error("trace:")
    logging.error(traceback.format_exc())