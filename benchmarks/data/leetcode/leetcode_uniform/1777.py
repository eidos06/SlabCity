import random
from database_generator.database_generator import DatabaseGenerator, generate_pair
import sys

dbsize = sys.argv[1]

# some setups
db_generator = DatabaseGenerator(size =dbsize, script_file=__file__,
                                 problem_id="1777")

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
1777 schema

Table: Products

+-------------+---------+
| Column Name | Type    |
+-------------+---------+
| product_id  | int     |
| store       | enum    |
| price       | int     |
+-------------+---------+
(product_id, store) is the primary key for this table.
store is an ENUM of type ('store1', 'store2', 'store3') where each represents the store this product is available at.
price is the price of the product at this store.
"""

# setup columns
total = size
product_id = list(range(int(total/2)))
store_pool = ['store1', 'store2', 'store3']
price = [random.randint(10, 20) for i in range(total)]

# fill in columns

product = []
product.append(['product_id','store','price'])
product.append(['int', 'str', 'int'])

pair_set = set()
for i in range(total):
    pair = generate_pair(product_id, store_pool)
    while pair in pair_set:
        pair = generate_pair(product_id, store_pool)
    pair_set.add(pair)
    product.append([pair[0], pair[1], random.choice(price)])

# output tables
db_generator.output("Products", product)
