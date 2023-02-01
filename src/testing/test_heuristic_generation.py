from heuristic_generation import Heuristic_Randon_Table_Generation
import json, yaml
import os
from util import print_db
from database_generator.database_generator import DatabaseGenerator
# read benchmark file
ids = []
with open('benchmarks_list.txt') as f:
    for row in f:
        pid, qid = row.strip().split('_')
        ids.append([pid, qid])
# correctness
# test constraint parse + filter
# ids = [['1270', '13']]
for Id in ids:
    pid, qid = Id
    # print(f'doing benchmark {pid}_{qid}')
    with open(f'../data/schema/{pid}.json') as f:
        schema = json.load(f)
    if os.path.exists(f'constraints/{pid}.yml'):
        with open(f'constraints/{pid}.yml') as f:
            cstr = yaml.safe_load(f)
    else:
        cstr = {}
    with open(f'../benchmarks/confirmed_benchmarks_2/{pid}_{qid}.json') as f:
        benchmark = json.load(f)
        query = benchmark['slow']['query']
    sizes = [300, 300, 300]
    
    generator = Heuristic_Randon_Table_Generation(schema, sizes, query, cstr)
    tables, db_schema = generator.generate()
    # print_db(generator.tables)

    db_generator = DatabaseGenerator(pid, 'testing_databases')
    db_generator.save_schema(db_schema)
    for tab_name in tables.keys():
        db_generator.output(tab_name, tables[tab_name], '0')
    
    generator = Heuristic_Randon_Table_Generation(schema, sizes, query, cstr, is_extreme=True)
    tables, _ = generator.generate()
    for tab_name in tables.keys():
        db_generator.output(tab_name, tables[tab_name], '1')
    