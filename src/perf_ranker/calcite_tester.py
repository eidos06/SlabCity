import psycopg
from typing import List
import logging
from enum import IntEnum
from itertools import permutations
from collections import Counter
import re
import time

def diff_table(data1, data2, consider_order=False):
    if len(data1) != len(data2):
        return True

    if consider_order:
        if data1 == data2:
            return False
    else:
        bag_semantic_data1 = dict(Counter(data1))
        bag_semantic_data2 = dict(Counter(data2))
        if bag_semantic_data1 == bag_semantic_data2:
            return False
    return True

class KillStatus(IntEnum):
    Killed = 0
    UnDetermined = 1
    NotKilled = 2

class PerfRanker:
    def __init__(self, config):
        self.config = config
        user_name = self.config["user_name"]
        password = self.config["password"]
        db_name = self.config["dbname"]
        schema_name = self.config["schema_name"]
        host = self.config["host"]
        self.conn = psycopg.connect(f"host={host} user={user_name} dbname={db_name} password={password} options='-c search_path={schema_name}' ",
                             autocommit=False)

    def run_query(self, queries:List[str]):
        result = []
        with self.conn.cursor() as cur:
            for q in queries:
                try:
                    cur.execute(q)
                    result.append(cur.fetchall())
                except Exception as e:
                    logging.warning(e)
        return result

    def explain_query(self, query):
        result = []
        with self.conn.cursor() as cur:
                try:
                    start = time.time()
                    cur.execute("EXPLAIN " + query)
                    result = cur.fetchall()
                    end = time.time()
                    EasyTesterManager.total_explain += 1
                    EasyTesterManager.total_time += (end-start)
                except Exception as e:
                    logging.warning(e)
        return result

    def explain_analysis(self, query):
        result = []  
        with self.conn.cursor() as cur:
                try:
                    cur.execute("EXPLAIN ANALYZE VERBOSE " + query)
                    result = cur.fetchall()
                except Exception as e:
                    logging.warning(e)
        return result



    def kill(self, query1, query2):
        outputs = self.run_query([query1, query2])
        if len(outputs[0]) == 0 and len(outputs[1]) == 0:
            return KillStatus.UnDetermined
        if diff_table(outputs[0], outputs[1], self.config["ordered"]):
            return KillStatus.Killed
        return KillStatus.NotKilled

    def get_explain_results(self, result):
        result_with_newline = ""
        for i in result:
            result_with_newline += i[0] + "\n"
        cost_string = result[0][0]
        r = re.search("cost=(.+)\.\.(.+)\srows=(.+)\swidth=(.+)\)", cost_string)
        start_cost = r.group(1)
        end_cost = r.group(2)
        num_rows = r.group(3)
        num_width = r.group(4)
        return result_with_newline, float(end_cost)


class PerfRankerManager:
    perf_ranker_map = {}
    total_time = 0
    total_explain = 0

    @classmethod
    def get_easy_tester(cls, problem_id, is_perf=False):
        if (problem_id, is_perf) in cls.easy_tester_map:
            return cls.easy_tester_map[(problem_id, is_perf)]
        if is_perf:
            config = {
                "host": "localhost",
                "user_name": "vldb",
                "password": "VLDB2023",
                "dbname": "calcite_perfrank",
                "schema_name": "problem" + problem_id,
                "ordered": True
            }
        cls.perf_ranker_map[problem_id] = PerfRanker(config)
        return cls.perf_ranker_map[problem_id]