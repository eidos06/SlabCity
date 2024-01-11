import random
from database_generator.database_generator import DatabaseGenerator, generate_pair
import sys

dbsize = sys.argv[1]

# some setups
db_generator = DatabaseGenerator(size =dbsize, script_file=__file__,
                                 problem_id="1126")

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
1126 schema

Table: Events

+---------------+---------+
| Column Name   | Type    |
+---------------+---------+
| business_id   | int     |
| event_type    | varchar |
| occurences    | int     | 
+---------------+---------+
(business_id, event_type) is the primary key of this table.
Each row in the table logs the info that an event of some type occurred at some business for a number of times.

"""

# setup columns
event_num = 100

bid_pool = list(range(int(size / 20)))
event_pool = ['event_' + str(x) for x in range(event_num)]
occurences = [random.randint(1, 30) for i in range(size)]

# prepare table

table = []
table.append(['business_id','event_type','occurences'])
table.append(['int', 'str', 'int'])

pair_set = set()

for i in range(size):
    pair = generate_pair(bid_pool, event_pool)
    while pair in pair_set:
        pair = generate_pair(bid_pool, event_pool)
    pair_set.add(pair)
    pair = list(pair)
    pair.append(occurences[i])
    table.append(pair)

# output tables
db_generator.output("Events", table)