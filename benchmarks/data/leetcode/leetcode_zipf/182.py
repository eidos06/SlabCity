import random
from database_generator.database_generator import DatabaseGenerator, zipf_transform
import sys

dbsize = sys.argv[1]

# some setups
db_generator = DatabaseGenerator(size =dbsize, script_file=__file__,
                                 problem_id="182")

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
182 schema

Table: Person

+-------------+---------+
| Column Name | Type    |
+-------------+---------+
| id          | int     |
| email       | varchar |
+-------------+---------+
id is the primary key column for this table.
Each row of this table contains an email. The emails will not contain uppercase letters.

"""

# setup columns

id = list(range(size))

email_pool = list(range(int(size / 1.5)))
email_pool = zipf_transform(email_pool)
email = ['email_' + str(random.choice(email_pool)) + '@cc.com' for _ in range(size)]

table = list(zip(id, email))
table.reverse()
table.append(['int','str'])
table.append(['id','email'])
table.reverse()

# output tables
db_generator.output("Person", table)
