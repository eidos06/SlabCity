import psycopg2 as psycopg
import os
from collections import OrderedDict
import logging
from testing.shared_connection import SharedConnection
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from .util import get_schema_name

def create_database(username, password, hostname, dbname):
    # print(f"creating database {dbname}")
    drop_database(username,password, hostname, dbname)
    conn = psycopg.connect(f"dbname=postgres user={username} password={password} host={hostname}")
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()
    cur.execute('CREATE DATABASE ' + dbname)
    cur.close()
    conn.close()
        


def drop_database(username, password, hostname, dbname): 
    conn = psycopg.connect(f"dbname=postgres user={username} password={password} host={hostname}")
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()
    cur.execute('DROP DATABASE IF EXISTS ' + dbname)
    cur.close()
    conn.close()

def drop_schema(schema_name):
    cur = SharedConnection.conn.cursor()
    # print(f"droping table")
    cur.execute('DROP SCHEMA ' + schema_name)
    cur.close()

def type_string(column, enums: list):
    """
    check size of varchar and add enum type
    """
    types = column["Type"].split(",")
    data_type = types[0]
    extra_info = ""
    if data_type == "varchar" and len(types) > 1 and types[1] == "size":
        assert(column["Size"])
        extra_info = f'({column["Size"]})'
    elif data_type == "enum":
        this_enum = set(types[1:])
        try:
            index = enums.index(this_enum)
        except:
            index = len(enums)
            enums.append(this_enum)
        data_type = f'enum{index}'
    return data_type + extra_info

def gen_create_drop_statement(schema, schema_name: str ="public"):
    """
    generate CREATE/DROP TABLE statements
    """
    table_schemas = schema["Tables"]
    create_tables = []
    drop_tables = []
    enums = []
    for table_schema in table_schemas:
        create_table = ["CREATE TABLE IF NOT EXISTS ", schema_name, ".", table_schema["TableName"], " ("]
        fields = []
        primary_key_names = set()
        for pkey in table_schema["PKeys"]:
            fields.append(f'{pkey["Name"]} {type_string(pkey, enums)}')
            primary_key_names.add(pkey["Name"])
        for fkey in table_schema["FKeys"]:
            if fkey["FName"] in primary_key_names: 
                # foreign key is also a primary key
                continue
            ptable_keys = table_schemas[int(fkey["PTable"])]["PKeys"]
            pkey = next(key for key in ptable_keys if key["Name"] == fkey["PName"])
            fields.append(f'{fkey["FName"]} {type_string(pkey, enums)}')
        for col in table_schema["Others"]:
            fields.append(f'{col["Name"]} {type_string(col, enums)}')
        if len(primary_key_names) > 0:
            pkey_list = ",".join(list(primary_key_names))
            fields.append(f"PRIMARY KEY ({pkey_list})")
        create_table.extend([", ".join(fields), ");"])
        create_tables.append("".join(create_table))
        drop_tables.append(f'DROP TABLE IF EXISTS {schema_name}.{table_schema["TableName"]};')
    create_types = []
    drop_types = []
    for i, enum in enumerate(enums):
        enum_string = ", ".join([f"'{item.lower()}'" for item in enum])
        create_types.append(f'CREATE TYPE enum{i} AS ENUM ({enum_string});')
        drop_types.append(f'DROP TYPE IF EXISTS enum{i} CASCADE;')
    create_statement = "\n".join(create_types + create_tables)
    drop_statement = "\n".join(drop_tables + drop_types)
    return create_statement, drop_statement

def value_string(value):
    if value == None or value == '' or value == 'null':
        return 'NULL'
    elif type(value) == int or type(value) == float:
        return str(value)
    else:
        return f"'{str(value).lower()}'"

def gen_insert_statement(schema, database: list, schema_name: str="public"):
    """
    generate INSERT INTO statements
    """
    table_schemas = schema["Tables"]
    insert_statements = []
    for table_schema, table in zip(table_schemas, database):
        insert_statement = ["INSERT INTO ", schema_name, ".", table_schema["TableName"], "\nVALUES\n"]
        columns = []
        for pkey in table_schema["PKeys"]:
            columns.append(pkey["Name"])
        for fkey in table_schema["FKeys"]:
            if fkey["FName"] in columns: 
                # foreign key is also a primary key
                continue
            columns.append(fkey["FName"])
        for col in table_schema["Others"]:
            columns.append(col["Name"])
        values = []
        num_row = min([len(table[column.lower()]) for column in columns])
        for i in range(num_row):
            value = [value_string(table[column.lower()][i]) for column in columns]
            
            values.append(f'({",".join(value)})')
        insert_statement.extend([",\n".join(values), ";"])
        insert_statements.append("".join(insert_statement))
    return "\n".join(insert_statements)

def run_psql(query, database: OrderedDict, schema, conf):
    """
    run query on database and return the output
    query can be a list of queries or a single string typed query
    """
    create_statement, drop_statement = gen_create_drop_statement(schema)
    insert_statement = gen_insert_statement(schema, database)
    commands = "\n".join([create_statement, insert_statement])
    with SharedConnection.conn.cursor() as cur:
        # cur.execute(drop_statement)
        # print(f"droping table")
        if 'timeout' in conf:
            timeout = "SET statement_timeout = " + str(conf["timeout"]) + ";"
            cur.execute(timeout)
        # print(commands)
        cur.execute(commands)
        # print(f"creating table")
        if type(query) == list:
            result = []
            for q in query:
                if type(q) != str:
                    raise ValueError(
                        "query argument must be string or list or strings"
                    )
                try:
                    cur.execute(q)
                except Exception as e:
                    result.append(e)
                    continue
                try:
                    result.append(cur.fetchall())
                except Exception as e:
                    print("!!!!!!!!!!!!!!!!")
                    print(e)
                    result.append(e)
                    logging.info(f"fail to fetch due to {e}")
                    logging.info(f"create statement\n {create_statement}")
                    logging.info(f"insert statement\n {insert_statement}")
                    logging.info(f"query\n {q}")
                    curs = SharedConnection.conn.cursor()
                    curs.execute("ROLLBACK")
        else:
            if type(query) != str:
                raise ValueError(
                    "query argument must be string or list or strings"
                )
            cur.execute(query)
            try:
                result = cur.fetchall()
            except Exception as e:
                print("!!!!!!!!!!!!!!!!")
                print(e)
                result.append(e)
                logging.info(f"fail to fetch due to {e}")
                logging.info(f"create statement\n {create_statement}")
                logging.info(f"insert statement\n {insert_statement}")
                logging.info(f"query\n {query}")
                curs = SharedConnection.conn.cursor()
                curs.execute("ROLLBACK")
        cur.execute(drop_statement)
        # print(f"droping table")
    return result

def run_psql_schema(query, schema_conn):
    """
    run query on database and return the output
    query can be a list of queries or a single string typed query
    """
    
    if type(query) == list:
        result = []
        for q in query:
            with schema_conn.cursor() as cur:
                if type(q) != str:
                    raise ValueError("query argument must be string or list or strings")
                try:
                    cur.execute(q)
                except Exception as e:
                    result.append(e)
                    logging.info(f"fail to fetch due to {e}")
                    logging.info(f"query\n {q}")
                    curs = schema_conn.cursor()
                    curs.execute("ROLLBACK")
                    continue
                try:
                    result.append(cur.fetchall())
                except Exception as e:
                    result.append(e)
                    logging.info(f"fail to fetch due to {e}")
                    logging.info(f"query\n {q}")
                    curs = schema_conn.cursor()
                    curs.execute("ROLLBACK")
    else:
        with schema_conn.cursor() as cur:
            if type(query) != str:
                raise ValueError("query argument must be string or list or strings")
            try:
                cur.execute(query)
            except Exception as e:
                result.append(e)
                logging.info(f"fail to fetch due to {e}")
                logging.info(f"query\n {query}")
                curs = schema_conn.cursor()
                curs.execute("ROLLBACK")
            try:
                result = cur.fetchall()
            except Exception as e:
                result.append(e)
                logging.info(f"fail to fetch due to {e}")
                logging.info(f"query\n {query}")
                curs = schema_conn.cursor()
                curs.execute("ROLLBACK")
    return result

def create_schema(schema_name):
    with SharedConnection.conn.cursor() as cur:
        cur.execute('CREATE SCHEMA ' + schema_name)

def create_conn(database: OrderedDict, schema, conf):
    """
    run query on database and return the output
    query can be a list of queries or a single string typed query
    """
    schema_name = get_schema_name()
    create_schema(schema_name)
    create_statement, _ = gen_create_drop_statement(schema, schema_name=schema_name)
    insert_statement = gen_insert_statement(schema, database, schema_name=schema_name)
    commands = "\n".join([create_statement, insert_statement])
    dbname = conf['dbname']
    username = conf['username']
    password = conf['password']
    host = conf['host']
    conn = psycopg.connect(f"dbname={dbname} user={username} password={password} host={host} options='-c search_path={schema_name}'")
    with conn.cursor() as cur:
        # cur.execute(drop_statement)
        # print(f"creating table")
        # print(commands)
        cur.execute(commands)
        conn.commit()
    return conn, schema_name

def explain_analyze_psql(query, database: OrderedDict, schema, conf):
    """
    EXPLAIN ANALYSE query
    return the cost and execution time
    """
    dbname = conf['dbname']
    username = conf['username']
    create_statement, drop_statement = gen_create_drop_statement(schema)
    insert_statement = gen_insert_statement(schema, database)
    commands = "\n".join([create_statement, insert_statement])
    os.system('./'+conf["clearcache_path"])
    with SharedConnection.conn.cursor() as cur:
        cur.execute(commands)
        if type(query) != str:
            raise ValueError(
                "query argument must be string"
            )
        timeout = "SET statement_timeout = " + str(conf["timeout"]) + ";"
        cur.execute(timeout)
        cur.execute("EXPLAIN ANALYZE " + query)
        result = cur.fetchall()
        
        # extract cost from output
        cost = result[0][0]
        pos = cost.find('.') + 5
        cost = cost[pos:-1]
        pos = cost.find(' ')
        cost = cost[0:pos]
        cost = float(cost)

        # extract runtime from output
        time = float(result[-1][0][16:-3])

        result = [cost, time]
        
        cur.execute(drop_statement)
    return result

def explain_query(query, database: OrderedDict, schema, conf):
    """
    EXPLAIN ANALYSE query
    return the cost and execution time
    """
    dbname = conf['dbname']
    username = conf['username']
    create_statement, drop_statement = gen_create_drop_statement(schema)
    insert_statement = gen_insert_statement(schema, database)
    commands = "\n".join([create_statement, insert_statement])
    with SharedConnection.conn.cursor() as cur:
        cur.execute(commands)
        if type(query) != str:
            raise ValueError(
                "query argument must be string"
            )
        try:
            cur.execute("EXPLAIN VERBOSE " + query)
            result = cur.fetchall()
            cur.execute(drop_statement)
        except:
            logging.info(f"fail to fetch due to {e}")
            logging.info(f"query\n {query}")
            curs = SharedConnection.conn.cursor()
            curs.execute("ROLLBACK")
            SharedConnection.conn.commit()
    return result