import random
from database_generator.database_generator import DatabaseGenerator, zipf_transform
import sys

dbsize = sys.argv[1]

# some setups
db_generator = DatabaseGenerator(size =dbsize, script_file=__file__,
                                 problem_id="1757")

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
1757 schema

Table: Products

+-------------+---------+
| Column Name | Type    |
+-------------+---------+
| product_id  | int     |
| low_fats    | enum    |
| recyclable  | enum    |
+-------------+---------+
product_id is the primary key for this table.
low_fats is an ENUM of type ('Y', 'N') where 'Y' means this product is low fat and 'N' means it is not.
recyclable is an ENUM of types ('Y', 'N') where 'Y' means this product is recyclable and 'N' means it is not.
"""

# setup columns
total = size

id = list(range(total))
boolean_pool = ['Y','N']

boolean_pool = zipf_transform(boolean_pool)


low_fats = [random.choice(boolean_pool) for _ in range(total)]
recyclable = [random.choice(boolean_pool) for _ in range(total)]

table = list(zip(id, low_fats, recyclable))
table.reverse()
table.append(['int','enum','enum'])
table.append(['product_id','low_fats','recyclable'])
table.reverse()

# output tables
db_generator.output("Products", table)

