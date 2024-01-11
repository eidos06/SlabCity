import random
from database_generator.database_generator import DatabaseGenerator, zipf_transform
import sys

dbsize = sys.argv[1]

# some setups
db_generator = DatabaseGenerator(size =dbsize, script_file=__file__,
                                 problem_id="1050")

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
1050 schema

Table: ActorDirector

+-------------+---------+
| Column Name | Type    |
+-------------+---------+
| actor_id    | int     |
| director_id | int     |
| timestamp   | int     |
+-------------+---------+
timestamp is the primary key column for this table.

"""

# setup columns
total = size

actor_num = 1000
director_num = int(size / 1000 / 3)

actor_pool = list(range(actor_num))
director_pool = list(range(director_num))

actor_pool = zipf_transform(actor_pool)
director_pool = zipf_transform(director_pool)

actor_id = [random.choice(actor_pool) for i in range(total)]
director_id = [random.choice(director_pool) for i in range(total)]
timestamp = list(range(total))


table = list(zip(actor_id, director_id, timestamp))
table.reverse()
table.append(['int','int','int'])
table.append(['actor_id','director_id','timestamp'])
table.reverse()
# output tables
db_generator.output("ActorDirector", table)