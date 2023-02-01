import json, yaml
from testing.tester import Tester
from testing.heuristic_generation import *
from testing.psql_util import drop_database, drop_schema
from testing.shared_connection import SharedConnection
import os
import psycopg2 as psycopg
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from faker import Faker

from testing.sqlite_util import run_query_conn, load_database_run_query, create_conn
from testing.util import dif_table
import time
from utils.time_collector import TimeCollector


class Oracle():
    def __init__(self, config_json, qid) -> None:
        self.config = config_json
        pid = self.config['problem']
        self.id = f"{pid}-{qid}"
        self.db_path = f"sqliteDBs/{self.id}"
        conf = self.config["tester"]["psql"]
        dbname = conf['dbname']
        username = conf['username']
        password= conf['password']
        host = conf['host']
        with open(f"{self.config['schema_path']}/{pid}.json") as f:
            self.schema = json.load(f)
        if os.path.exists(f"{self.config['constraint_path']}/{pid}.yaml"):
            with open(f"{self.config['constraint_path']}/{pid}.yaml") as f:
                self.cstr = yaml.safe_load(f)
        else:
            self.cstr = {}
        self.tester = Tester(self.schema, self.config['tester'], self.cstr)
        SharedConnection.conn = psycopg.connect(f"dbname={dbname} user={username} password={password} host={host}")
        SharedConnection.conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        # init shared database
        self.generator_1 = None
        self.generator_2 = None
        self.generator_3 = None
        self.fake = Faker()
        self.truth_outputs = {}
        # print(self.fake)

    def init_generator(self, truth):
        self.generator_1 = Heuristic_Random_Table_Generation(
                    self.schema, self.config['sizes'], truth, self.fake, raw_cstr=self.cstr
                )
        self.generator_2 = Heuristic_Random_Table_Generation(
            self.schema, self.config['sizes'], truth, self.fake, raw_cstr=self.cstr, is_extreme=True
        )
        self.generator_3 = Heuristic_Random_Table_Generation(
            self.schema, self.config['sizes'], truth, self.fake, raw_cstr=self.cstr, enable_group_by=True
        )

    def setup_sqlite(self):
        if not os.path.exists(self.db_path):
            os.makedirs(self.db_path)

    def clean_sqlite(self):
        if os.path.exists(self.db_path):
            os.system(f"rm -rf {self.db_path}")

    def clean(self):
        SharedConnection.conn.close()
        drop_database(self.config["tester"]["psql"]["username"], self.config["tester"]["psql"]["password"],
                      self.config["tester"]["psql"]["host"], self.config["tester"]["psql"]["dbname"])        

    def test(self, query: str, truth: str, conns: list):
        """test whether query and truth are equivalent
        Input:
        - query: the candidate query to be tested
        - truth: the ground truth query
        - databases: a list of schema names in postgres
        - mode: 'heu' for heuristics, others for random
        Return: bool + []
        - bool: True if query is equivalent to truth, otherwise False (non-equv or error)
        - []: a list of schema names, the superset of input databases
        """
        # try to kill the query with input databases
        start = time.time()
        res, _, _ = self.tester.buffer_testing_conn(truth, query, conns, order=self.config['ordered'])
        end = time.time()
        TimeCollector.time_checking_against_counterexamples += end-start
        
        if res[0] == True:
            TimeCollector.num_use_counter_example += 1 
            return False, conns
        elif res[0] == 'error':
            raise Exception(f'fail to run candidate query {query}')
        else:
            TimeCollector.num_use_full_check += 1
            start = time.time()
            database_1, database_2, database_3 = self.generate_dbs()
            self.tester.database_buffer = [database_1, database_2, database_3]
            res, _, dbs = self.tester.buffer_testing(truth, query, self.config['ordered'])
            end = time.time()
            TimeCollector.time_equivlance_checker += end-start
            # completing testing
            if res[0] == True:
                conn = self.tester.build_conns(dbs)
                TimeCollector.counterexample_generated += len(conn)
                conns += conn
                return False, conns
            elif res[0] == 'error':
                return False, conns
            else:
                return True, conns

    def generate_dbs(self):
        self.generator_1.generate()
        database_1 = self.generator_1.tables
        self.generator_2.generate()
        database_2 = self.generator_2.tables
        self.generator_3.generate()
        database_3 = self.generator_3.tables
        return database_1,database_2,database_3

    def test_sqlite(self, query: str, truth: str, conns: list):
        # test query using counter examples
        for db_name, conn in conns:
            results = run_query_conn(query, conn)
            if dif_table(results, self.truth_outputs[db_name]):
                return False, conns
        # generate new dbs
        databases = self.generate_dbs()
        for database in databases:
            # run truth on database
            result_truth = load_database_run_query(database, self.schema, self.db_path, truth)
            result_query = load_database_run_query(database, self.schema, self.db_path, query)
            if dif_table(result_query, result_truth):
                conn, db_name = create_conn(database, self.schema, self.db_path)
                self.truth_outputs[db_name] = result_truth
                conns.append([db_name, conn])
                return False, conns
        return True, conns

    def clear_counterexample_sqlite(self, conns):
        for _, conn in conns:
            conn.close()

    def clear_counterexample(self, conns):
        for conn, schema_name in conns:
            # print(f"closing connection to schema {schema_name}")
            conn.close()
            # print(f"droping schema {schema_name}")
            drop_schema(schema_name)
