import random
from database_generator.database_generator import DatabaseGenerator, zipf_transform
import sys

dbsize = sys.argv[1]

# some setups
db_generator = DatabaseGenerator(size =dbsize, script_file=__file__,
                                 problem_id="181")

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
181 schema

Table: Employee

+-------------+---------+
| Column Name | Type    |
+-------------+---------+
| id          | int     |
| name        | varchar |
| salary      | int     |
| managerId   | int     |
+-------------+---------+
id is the primary key column for this table.
Each row of this table indicates the ID of an employee, their name, salary, and the ID of their manager.

"""

# setup columns

id = list(range(size))
name_pool = ['name_' + str(x) for x in range(int(size / 10))]
salary_pool = [x for x in range(4000, 5000)]
manager_num = int(size / 5)
managerid_pool = [random.choice(id) for i in range(manager_num)]


salary_pool = zipf_transform(salary_pool)
managerid_pool = zipf_transform(managerid_pool)

name = [random.choice(name_pool) for i in range(size)]
salary = [random.choice(salary_pool) for i in range(size)]
managerId = [random.choice(managerid_pool) for i in range(size)]


table = list(zip(id, name, salary, managerId))
table.reverse()
table.append(['int','str','int','int'])
table.append(['id','name','salary','managerid'])
table.reverse()

# output tables
db_generator.output("Employee", table)