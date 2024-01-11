import random
from database_generator.database_generator import DatabaseGenerator, zipf_transform
import sys

dbsize = sys.argv[1]

# some setups
db_generator = DatabaseGenerator(size =dbsize, script_file=__file__,
                                 problem_id="184")

seed = 2333
random.seed(seed)

size = 1000000

if dbsize == '10M':
    size = 10000000
elif dbsize == '1M':
    size = 1000000
elif dbsize == '100K':
    size = 100000

dept_num = int(size / 1000)

eid_pool = [i for i in range(size)]
did_pool = [i for i in range(dept_num)]
name_pool = ['ename_' + str(i) for i in range(int(size / 2))]
department_name_pool = ['dname_' + str(i) for i in range(dept_num)]
salary_pool = [i for i in range(100, 150)]

transformed_did_pool = zipf_transform(did_pool)
name_pool = zipf_transform(name_pool)
department_name_pool = zipf_transform(department_name_pool)
salary_pool = zipf_transform(salary_pool)

employee_table = []
employee_table.append(['id','departmentid','name','salary'])
employee_table.append(['int','int','str','int'])
for pid in eid_pool:
    employee_table.append([pid, random.choice(transformed_did_pool), random.choice(name_pool), random.choice(salary_pool)])
department_table = []
department_table.append(['id','name'])
department_table.append(['int', 'str'])
for did in did_pool:
    department_table.append([did, random.choice(department_name_pool)])

# output tables
db_generator.output("Employee", employee_table)
db_generator.output("Department", department_table)
