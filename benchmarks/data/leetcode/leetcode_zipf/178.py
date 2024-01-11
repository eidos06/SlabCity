import random
from database_generator.database_generator import DatabaseGenerator, zipf_transform
import sys

dbsize = sys.argv[1]

# some setups
db_generator = DatabaseGenerator(size =dbsize, script_file=__file__,
                                 problem_id="178")

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
178 schema

Table: Scores

+-------------+---------+
| Column Name | Type    |
+-------------+---------+
| id          | int     |
| score       | decimal |
+-------------+---------+
id is the primary key for this table.
Each row of this table contains the score of a game. Score is a floating point value with two decimal places.

"""

# setup columns

score_num = int(size / 5)
score_pool = [float(x) for x in range(score_num)]

transformed_score_pool = zipf_transform(score_pool)

# fill in columns

id = list(range(size))
score = [random.choice(transformed_score_pool) for _ in range(size)]

# prepare table
table = list(zip(id, score))
table.reverse()
table.append(['int','decimal'])
table.append(['id', 'score'])
table.reverse()
# output tables
db_generator.output("Scores", table)