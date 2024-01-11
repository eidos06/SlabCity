import random
from database_generator.database_generator import DatabaseGenerator
import sys


dbsize = sys.argv[1]

# some setups
db_generator = DatabaseGenerator(size =dbsize, script_file=__file__,
                                 problem_id="570")

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


"""
570 schema

Table: Employee

+-------------+---------+
| Column Name | Type    |
+-------------+---------+
| id          | int     |
| name        | varchar |
| department  | varchar |
| managerId   | int     |
+-------------+---------+
id is the primary key column for this table.
Each row of this table indicates the name of an employee, their department, and the id of their manager.
If managerId is null, then the employee does not have a manager.
No employee will be the manager of themself.

"""

# setup columns
total = size

id = list(range(total))
name_pool = ['name_' + str(x) for x in range(int(total / 10))]
department_pool = ['d_' + str(x) for x in range(3000)]
manager_num = int(total / 5)
managerid_pool = [random.choice(id) for i in range(manager_num)]

name = [random.choice(name_pool) for i in range(total)]
department = [random.choice(department_pool) for i in range(total)]
managerId = [random.choice(managerid_pool) for i in range(total)]


table = list(zip(id, name, department, managerId))
table.reverse()
table.append(['int','str','str','int'])
table.append(['id','name','department','managerid'])
table.reverse()

# output tables
db_generator.output("Employee", table)