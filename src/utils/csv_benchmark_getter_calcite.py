import os
import csv
import re
import json
import glob
from typing import List
from lib.basics import Schema
from lib.dsl import Table
from lib.types import DataType


class CsvBenchmark:
    def __init__(self, problem_id: str, query_id: str, query_text: str):
        self.problem_id = problem_id
        self.query_id = query_id
        self.query_text = query_text


def get_schema_by_json(schema_json):
    tables = []

    # read Pkeys and others first

    for idx, t in enumerate(schema_json["Tables"]):
        tables.append(Table(table_name=t["TableName"], table_cols=[]))

        all_regular_keys = t["PKeys"] + t["Others"]
        for c in all_regular_keys:
            colType = None
            if c["Type"] == "int":
                colType = DataType.Number
            elif c["Type"] == "varchar":
                colType = DataType.Str
            else:
                colType = DataType.Str
            tables[idx].table_cols.append((c["Name"], colType))

    # read Fkeys next

    for idx, t in enumerate(schema_json["Tables"]):
        for c in t["FKeys"]:
            colname = c["FName"]
            coltype = None
            for col in tables[int(c["PTable"])].table_cols:
                if col[0] == c["PName"]:
                    coltype = col[1]
                    break
            tables[idx].table_cols.append((colname, coltype))
    return_schema = Schema()
    for t in tables:
        return_schema.add_table(t)
    return return_schema


class CsvBenchmarkGetter:
    def __init__(self, benchmark_folder="", schema_folder=""):
        self.schemas = {}
        self.benchmarks = []
        # print("getting benchmarks...")
        # get schemas
        if schema_folder != "":
            file_list = os.listdir(schema_folder)
            for file_name in file_list:
                full_file_name = schema_folder + "/" + file_name
                with open(full_file_name) as f:
                    schema_content = json.load(f)
                    schema_id = schema_content["Problem Number"]

                    self.schemas[schema_id] = get_schema_by_json(schema_content)
        # get benchmarks
        if benchmark_folder != "":
            self.benchmarks: List[CsvBenchmark] = []
            benchmark_files = glob.glob(benchmark_folder + "/**/*.csv", recursive=True)
            for file in benchmark_files:
                with open(file) as f:
                    benchmark_csv = csv.reader(f)
                    problem_id = str(re.search("([^\/\.]+).csv", file).group(1))
                    # skip first line
                    # next(benchmark_csv, None)
                    for row in benchmark_csv:
                        query_id = str(row[0])
                        query_text = str(row[1])
                        self.benchmarks.append(CsvBenchmark(problem_id, query_id, query_text))

        # print("\nfinished getting benchmarks")

    def get_schema_by_benchmark_name(self, benchmark_name):
        format_checker = benchmark_name.find("runtime")
        benchmark_id = benchmark_name
        if format_checker != -1:
            benchmark_id = benchmark_name[benchmark_name.find("runtime") + len("runtime"):benchmark_name.find(".csv")]
        return self.schemas[benchmark_id]

    def get_query_text_by_pid_and_qid(self, pid, qid):
        for b in self.benchmarks:
            if b.problem_id != pid:
                continue
            if b.query_id == qid:
                return b.query_text
        raise Exception("can't find query " + pid + ":" + qid)

    def get_all_benchmarks_by_problems(self):
        problem_dic = {}
        for b in self.benchmarks:
            if b.problem_id in problem_dic:
                problem_dic[b.problem_id].append(b)
            else:
                problem_dic[b.problem_id] = []
        return problem_dic.items()
