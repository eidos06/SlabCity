import random
from database_generator.database_generator import DatabaseGenerator, zipf_transform
import sys

dbsize = sys.argv[1]

# some setups
db_generator = DatabaseGenerator(size =dbsize, script_file=__file__,
                                 problem_id="1303")

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
1303 schema

Table: Employee

+---------------+---------+
| Column Name   | Type    |
+---------------+---------+
| employee_id   | int     |
| team_id       | int     |
+---------------+---------+
employee_id is the primary key for this table.
Each row of this table contains the ID of each employee and their respective team.

"""

# setup columns
total = size

team_num = int(total / 100)
team_id_pool = [i for i in range(team_num)]

team_id_pool = zipf_transform(team_id_pool)

# fill in columns
employee_id = [i for i in range(total)]
team_id = [random.choice(team_id_pool) for i in range(total)]

table = list(zip(employee_id, team_id))
table.reverse()
table.append(['int','int'])
table.append(['employee_id','team_id'])
table.reverse()
# output tables
db_generator.output("Employee", table)