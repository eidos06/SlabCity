import random
from database_generator.database_generator import DatabaseGenerator, generate_pair, zipf_transform
import sys

dbsize = sys.argv[1]

# some setups
db_generator = DatabaseGenerator(size =dbsize, script_file=__file__,
                                 problem_id="1077")

# setup seed
seed = 2333
random.seed(seed)

size = 1000000

if dbsize == '10M':
    size = 10000000
elif dbsize == '1M':
    size = 1000000
elif dbsize == '100K':
    size = 100000

# start logic writing

employee_num = int(size / 2)
name_num = int(employee_num * 0.6)
project_num = int(size / 1000)

employee_id_pool = list(range(employee_num))
name_pool = [str(x) for x in list(range(-name_num, 0))]
project_id_pool = list(range(project_num))
years_pool = [i for i in range(1, 10)]


transformed_employee_id_pool = zipf_transform(employee_id_pool)
project_id_pool = zipf_transform(project_id_pool)
years_pool = zipf_transform(years_pool)
name_pool = zipf_transform(name_pool)

project_table = []
project_table.append(['project_id','employee_id'])
project_table.append(['int', 'int'])
employee_table = []
employee_table.append(['employee_id','name','experience_years'])
employee_table.append(['int','str','int'])

pair_set = set()

for i in range(size):
    pair = generate_pair(project_id_pool, transformed_employee_id_pool)
    while pair in pair_set:
        pair = generate_pair(project_id_pool, transformed_employee_id_pool)
    pair_set.add(pair)
    project_table.append(list(pair))

for id in employee_id_pool:
    employee_table.append([id, random.choice(name_pool), random.choice(years_pool)])

# output tables
db_generator.output("Project", project_table)
db_generator.output("Employee", employee_table)
