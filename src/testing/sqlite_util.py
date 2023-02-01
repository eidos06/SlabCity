import sqlite3
from collections import OrderedDict
from testing.util import get_schema_name

def type_string(column, enums: list):
    """
    check size of varchar and add enum type
    """
    types = column["Type"].split(",")
    data_type = types[0]
    if data_type == "int":
        return "INTEGER"
    elif data_type == "varchar":
        return "TEXT"
    elif data_type == "numeric":
        return "REAL"
    else:
        return "TEXT" 

def gen_create_drop_statement(schema, db_name: str ="public"):
    """
    generate CREATE/DROP TABLE statements
    """
    table_schemas = schema["Tables"]
    create_tables = []
    drop_tables = []
    enums = []
    for table_schema in table_schemas:
        create_table = ["CREATE TABLE IF NOT EXISTS ", table_schema["TableName"], " ("]
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
        drop_tables.append(f'DROP TABLE IF EXISTS {db_name}.{table_schema["TableName"]};')
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

def gen_insert_statement(schema, database: list, db_name: str="public"):
    """
    generate INSERT INTO statements
    """
    table_schemas = schema["Tables"]
    insert_statements = []
    for table_schema, table in zip(table_schemas, database):
        insert_statement = ["INSERT INTO ", table_schema["TableName"], "\nVALUES\n"]
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
        num_row = len(table[columns[0].lower()])
        for i in range(num_row):
            value = [value_string(table[column.lower()][i]) for column in columns]
            
            values.append(f'({",".join(value)})')
        insert_statement.extend([",\n".join(values), ";"])
        insert_statements.append("".join(insert_statement))
    return "\n".join(insert_statements)

def run_query_conn(query, conn):
    cur = conn.cursor()
    res = cur.execute(query)
    return res.fetchall()

def load_database_run_query(database: OrderedDict, schema: dict, db_path: str, query: str):
    db_name = 'testing'
    create_statements, drop_statements = gen_create_drop_statement(schema, db_name)
    insert_statements = gen_insert_statement(schema, database, db_name)
    import os
    conn = sqlite3.connect(f"{db_path}/{db_name}")
    cur = conn.cursor()
    create_statements = create_statements.split(';')
    insert_statements = insert_statements.split(';')
    statements = create_statements + insert_statements
    for statement in statements:
        cur.execute(statement)
    res = cur.execute(query)
    output = res.fetchall()
    cur.execute(drop_statements)
    cur.close()
    conn.close()
    return output

def create_conn(database: OrderedDict, schema: dict, db_path: str):
    db_name = get_schema_name()
    create_statements, _ = gen_create_drop_statement(schema, db_name)
    insert_statements = gen_insert_statement(schema, database, db_name)
    conn = sqlite3.connect(f"{db_path}/{db_name}")
    cur = conn.cursor()
    cur.execute(create_statements)
    cur.execute(insert_statements)
    cur.close()
    return conn, db_name

