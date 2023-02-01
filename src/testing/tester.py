from typing import Union
from testing.util import dif_table, readable_db
from testing.psql_util import run_psql,create_database, gen_create_drop_statement, gen_insert_statement, create_conn
from testing.psql_util import run_psql_schema
from testing.constrained_generation import Constrained_Random_Table_Generation
import time
import logging
import timeit
import os, datetime, csv
from collections import defaultdict


class Tester():
    def __init__(self, schema, conf, constraints, engin='psql') -> None:
        self.schema = schema
        self.conf = conf
        self.raw_cstr = constraints
        self.engin = engin
        self.database_buffer = []
        self.database_conn_buffer = []
        create_database(self.conf["psql"]["username"], self.conf["psql"]["password"], self.conf["psql"]["host"], self.conf["psql"]["dbname"])

    def run_query(self, query, testing_database):
        """
        run a query or a list of queries on the testing database in SQL engin
        currently only support psql
        """
        return run_psql(query, testing_database, self.schema, self.conf["psql"])

    def run_query_conn(self, query, schema_conn):
        """
        run a query or a list of queries on the testing database in SQL engine
        with speficied connection
        currently only support psql
        """
        return run_psql_schema(query, schema_conn)

    def build_conns(self, databases):
        conns = []
        for database in databases:
            conn, schema_name = create_conn(database, self.schema, self.conf['psql'])
            conns.append([conn, schema_name])
        return conns

    def close_all_conn(self):
        for conn in self.database_conn_buffer:
            conn.close()

    def generate_databse(self, is_random=True):
        """
        generate a database that follows the schema
        currently only support random generation
        """
        if is_random:
            seed = time.time()
            if self.conf["log_seed"]:
                logging.info(f"using seed={seed}")
            database_generator = Constrained_Random_Table_Generation(
                self.schema,
                self.conf['sizes'],
                self.raw_cstr,
                seed=seed
            )
            database = database_generator.generate()
            return database
        else:
            raise NotImplementedError

    def random_kill(self, query1, query2,
                    limit_type: str = 'trial',  # trial or timeout
                    limit_quantity: int = 60,  # number of trials or timeout in seconds
                    order: bool = False,  # if require the row order of outputs to be the same
                    nonempty: bool = False  # if require the output of at least one query to be non-empty
                    ):
        """
        try to randomly find a database in either fixed # trials or time
        on which the truth_query and test_query generate different outputs
        if found, return the database and number of trials
        otherwise, return None
        """
        killed, found_nonempty, logging_output = False, False, ""
        if limit_type == 'trial':
            limits = 0
        elif limit_type == 'timeout':
            start_time = timeit.default_timer()

        while True:
            testing_database = self.generate_databse(is_random=True)
            queries = [query1, query2]
            outputs = self.run_query(queries, testing_database)
            if nonempty:
                if len(outputs[0]) == 0 and len(outputs[1]) == 0:
                    continue
                # print("nonempty")
            found_nonempty = True
            if dif_table(outputs[0], outputs[1], order):
                killed = True
                logging_output += f"killed by\n {readable_db(testing_database)}"
                logging_output += f"outputs are\n{outputs[0]}\n{outputs[1]}"
                if limit_type == 'timeout':
                    limits = timeit.default_timer() - start_time
                elif limit_type == 'trial':
                    limits += 1
                break
            else:
                if limit_type == 'timeout':
                    if timeit.default_timer() - start_time > limit_quantity:
                        break
                elif limit_type == 'trial':
                    if limits > limit_quantity:
                        break
        if killed:
            return testing_database, limits, logging_output
        elif not nonempty:
            return None, limit_quantity, \
                "not found database to distinguish two queries"
        elif found_nonempty:
            return None, limit_quantity, \
                "found database to generate nonempty output, but fail to distinguish the queries"
        else:
            return None, limit_quantity, \
                "not found database to generate nonempty output, and fail to distinguish the queries"

    def random_testing(self, reference: str, testing: Union[str, list],
                       order: bool = False,
                       limit_type: str = 'trial',
                       limit_quantity: int = 60,
                       nonempty: bool = False
                       ):
        """
        randomly test a list of queries or one query
        return two lists:
        1. results of testing: False if two queries are distinguished
        2. results of extra_info: {'limits': limit_quantity, 'outputs': logging_output}
        """
        if isinstance(testing, list):
            results = []
            extra_infos = []
            useful_dbs = []
            for query in testing:
                res, extra_info, useful_db = self.random_testing(reference, query)
                results += res
                extra_infos += extra_info
                useful_dbs += useful_db
            return results, extra_infos, useful_dbs
        else:
            for database in self.database_buffer:
                outputs = self.run_query([reference, testing], database)
                if dif_table(outputs[0], outputs[1], order):
                    return [True], [{}]
            # not distinguished by existing databases
            database, limits, logging_output = self.random_kill(
                reference, testing, limit_type=limit_type,
                limit_quantity=limit_quantity, order=order,
                nonempty=nonempty
            )
            extra_info = {
                'limits': limits,
                'outputs': logging_output
            }
            if database != None:
                self.database_buffer.append(database)
                return [True], [extra_info], [database]
            else:
                return [False], [extra_info], [database]

    def kill_mutants(self, reference: str, mutants: list,
                     order: bool = False,
                     limit_type: str = 'trial',
                     limit_quantity: int = 60
                    ):
        logging_outputs = []
        if limit_type == 'trial':
            limits = 0
        elif limit_type == 'timeout':
            start_time = timeit.default_timer()
        indication = [0 for _ in range(len(mutants))]
        while True:
            useful = False
            testing_database = self.generate_databse(is_random=True)
            for i, mutant in enumerate(mutants):
                if indication[i] == 1:
                    continue
                try:
                    outputs = self.run_query([reference, mutant], testing_database)
                except Exception as e:
                    # print(f"fail to run query as {e}")
                    indication[i] = 1
                    continue
                if dif_table(outputs[0], outputs[1], order):
                    indication[i] = 1
                    if not useful:
                        self.database_buffer.append(testing_database)
                        useful = True
                    # print(f"kill mutant {i}")
            if sum(indication) == len(indication):
                logging_outputs = "all mutant killed"
                break
            if limit_type == 'trial':
                limits += 1
            elif limit_type == 'timeout':
                limits = timeit.default_timer() - start_time
            if limits > limit_quantity:
                not_killed = [i for i in range(len(indication)) if indication[i] == 0]
                logging_outputs = f"{len(not_killed)/len(indication)} mutants {not_killed} are not killed in limits {limit_quantity}"
                break
        return logging_outputs
    
    def save_database_buffer(self, path):
        for i, database in enumerate(self.database_buffer):
            for j, table in enumerate(database):
                if not os.path.exists(f"{path}/{str(i)}"):
                    os.makedirs(f"{path}/{str(i)}")
                keys, types, values = [], [], []
                for key, value in table.items():
                    keys.append(key)
                    values.append(value)
                for value in values:
                    if type(value[0]) == int:
                        types.append('int')
                    elif type(value[0]) == datetime.date:
                        types.append('date')
                    else:
                        types.append('str')
                values = list(map(list, zip(*values)))
                with open(f"{path}/{str(i)}" + \
                    f"/{self.schema['Tables'][j]['TableName']}.csv", "w") \
                    as f:
                    writer = csv.writer(f)
                    writer.writerow(keys)
                    writer.writerow(types)
                    writer.writerows(values)

    def load_database_buffer(self, path):
        self.database_buffer = []
        i = 0
        while True:
            tables = [defaultdict(list) for _ in range(len(self.schema["Tables"]))]
            database_path = path + "/" + self.schema["Problem Number"] + "/" + str(i)
            if os.path.exists(database_path):
                for table, table_schema in zip(tables, self.schema["Tables"]):
                    table_name = table_schema["TableName"]
                    with open(database_path + "/" + table_name + ".csv", 'r') as f:
                        reader = csv.reader(f)
                        header = next(reader)
                        types = next(reader)
                        values = []
                        for row in reader:
                            values.append(row)
                    values = list(map(list, zip(*values)))
                    for i, t in enumerate(types):
                        if t == 'int':
                            values[i] = [int(v) for v in values[i]]
                        elif t == 'date':
                            values[i] = [datetime.datetime.strptime(v, "%Y-%m-%d").date() \
                                for v in values[i]]
                    assert(len(values) == len(header))
                    for key, value in zip(header, values):
                        table[key] = value
                self.database_buffer.append(tables)
                i += 1
            else:
                break

    
    def load_one_database(self, path):
        # under the path, we need several table_name.csv and a schema.txt
        database = []
        database_schema = {}
        # read schema
        with open(f'{path}/schema.txt') as f:
            for row in f:
                if ',' not in row:
                    database_schema[row.strip()] = []
                    current_table = row.strip()
                else:
                    cols = row.strip().split(',')[:-1]
                    database_schema[current_table] = cols
        # read tables
        inc = 0
        while True:
            if not os.path.exists(f'{path}/{inc}'):
                break
            for table in self.schema['Tables']:
                table_name = table['TableName'].lower()
                values = []
                with open(f'{path}/{inc}/{table_name}.csv') as f:
                    reader = csv.reader(f)
                    for row in reader:
                        values.append(row)
                values = list(zip(*values))
                col_names = database_schema[table_name]
                table = defaultdict(list)
                assert(len(values) == len(col_names))
                for value, col in zip(values, col_names):
                    table[col] = value
                database.append(table)
            inc += 1
        self.database_buffer.append(database)

    def buffer_testing_conn(self, truth, query, conns, order: bool = False):
        for conn, _ in conns:
            outputs = self.run_query_conn([truth, query], conn)
            if isinstance(outputs[0], Exception) or isinstance(outputs[1], Exception):
                    return [True], [{'output': f'error when running the query {outputs}'}], []
            if dif_table(outputs[0], outputs[1], order):
                return [True], [], [conn]
        return [False], [{}], []
    
    def buffer_testing(self, reference, testing: Union[str, list], order: bool = False):
        if isinstance(testing, list):
            results = []
            extra_infos = []
            useful_dbs = []
            for query in testing:
                res, extra_info, useful_db = self.random_testing(reference, query)
                results += res
                extra_infos += extra_info
                useful_dbs += useful_db
            return results, extra_infos, useful_dbs
        else:
            for database in self.database_buffer:
                outputs = self.run_query([reference, testing], database)
                # print(outputs)
                if isinstance(outputs[0], Exception) or isinstance(outputs[1], Exception):
                    return [True], [{'output': f'error when running the query {outputs}'}], []
                if dif_table(outputs[0], outputs[1], order):
                    # logging.info(outputs)
                    logging_output = f"killed by\n{readable_db(database)}"
                    logging_output += f"outputs are\n{outputs[0]}\n{outputs[1]}"
                    return [True], [{'outputs': logging_output}], [database]
            return [False], [{}], []
    
    def get_create_drop_stmt(self, engin='psql'):
        if engin == 'psql':
            return gen_create_drop_statement(self.schema)
        return 

    def get_insert_stmt(self, engin='psql', path='database'):
        # get database
        if len(self.database_buffer) == 0:
            self.load_database_buffer('database')
            if len(self.database_buffer) == 0:
                database = self.generate_databse()
                self.database_buffer.append(database)
        database = self.database_buffer[0]
        if engin == 'psql':
            return gen_insert_statement(self.schema, database)
