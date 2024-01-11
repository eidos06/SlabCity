import random

from database_generator.database_generator import DatabaseGenerator
import sys

dbsize = sys.argv[1]

# some setups
db_generator = DatabaseGenerator(size =dbsize, script_file=__file__,
                                 problem_id="185")

seed = 2333
random.seed(seed)

size = 1000000

if dbsize == '10M':
    size = 10000000
elif dbsize == '1M':
    size = 1000000
elif dbsize == '100K':
    size = 100000


department_num = int(size / 100)
employee_num = size

d_id_pool = list(range(department_num))
d_name_pool = ['d_name_' + str(i) for i in range(department_num)]
e_name_pool = ['e_name_' + str(i) for i in range(int(employee_num * 0.6))]
salary_pool = list(range(950, 1000))

# start logic writing

employee_table = []
employee_table.append(['id','departmentid','name','salary'])
employee_table.append(['int', 'int','str','int'])
for i in range(employee_num):
    employee_table.append([i, random.choice(d_id_pool), random.choice(e_name_pool), random.choice(salary_pool)])

department_table = []
department_table.append(['id', 'name'])
department_table.append(['int', 'str'])
for id in d_id_pool:
    department_table.append([id, random.choice(d_name_pool)])

# output tables
db_generator.output("Employee", employee_table)
db_generator.output("Department", department_table)
