import random
from database_generator.database_generator import DatabaseGenerator, zipf_transform
import sys

dbsize = sys.argv[1]

# some setups
db_generator = DatabaseGenerator(size =dbsize, script_file=__file__,
                                 problem_id="1378")

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
1378 schema

Table: Employees

+---------------+---------+
| Column Name   | Type    |
+---------------+---------+
| id            | int     |
| name          | varchar |
+---------------+---------+
id is the primary key for this table.
Each row of this table contains the id and the name of an employee in a company.

Table: EmployeeUNI

+---------------+---------+
| Column Name   | Type    |
+---------------+---------+
| id            | int     |
| unique_id     | int     |
+---------------+---------+
(id, unique_id) is the primary key for this table.
Each row of this table contains the id and the corresponding unique id of an employee in the company.

"""

# setup columns
total = size

id = list(range(total))
name_pool = ['name_' + str(x) for x in range(int(total / 10))]

name_pool = zipf_transform(name_pool)
transformed_id = zipf_transform(id)

name = [random.choice(name_pool) for i in range(total)]

uni_num = int(total / 5)
unique_id = list(range(uni_num))
sub_id = [random.choice(transformed_id) for i in range(uni_num)]

employees_table = list(zip(id, name))
employees_table.reverse()
employees_table.append(['int','str'])
employees_table.append(['id','name'])
employees_table.reverse()

uni_table = list(zip(sub_id, unique_id))
uni_table.reverse()
uni_table.append(['int','int'])
uni_table.append(['id','unique_id'])
uni_table.reverse()

# output tables
db_generator.output("Employees", employees_table)
db_generator.output("EmployeeUNI", uni_table)
