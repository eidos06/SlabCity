import random
from database_generator.database_generator import DatabaseGenerator, generate_pair
import sys

dbsize = sys.argv[1]

# some setups
db_generator = DatabaseGenerator(size =dbsize, script_file=__file__,
                                 problem_id="1729")

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
1729 schema

Table: Followers

+-------------+------+
| Column Name | Type |
+-------------+------+
| user_id     | int  |
| follower_id | int  |
+-------------+------+
(user_id, follower_id) is the primary key for this table.
This table contains the IDs of a user and a follower in a social media app where the follower follows the user.

"""

# setup columns
total = size
user_num = int(total ** 0.5 * 10)

user_pool = [i for i in range(user_num)]

# fill in columns

table = []
table.append(['user_id','follower_id'])
table.append(['int', 'int'])

pair_set = set()
for i in range(total):
    pair = generate_pair(user_pool, user_pool)
    while pair in pair_set or pair[0] == pair[1]:
        pair = generate_pair(user_pool, user_pool)
    pair_set.add(pair)
    table.append([pair[0],pair[1]])

# output tables
db_generator.output("Followers", table)


