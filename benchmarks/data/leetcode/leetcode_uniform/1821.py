import random
from database_generator.database_generator import DatabaseGenerator, generate_pair
import sys

dbsize = sys.argv[1]

# some setups
db_generator = DatabaseGenerator(size =dbsize, script_file=__file__,
                                 problem_id="1821")

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

# start logic writing
tup_set = set()

id_pool = [i for i in range(size)]
year_pool = [i for i in range(2000, 2022)]
rev_pool = [i for i in range(-15, 15)]

customers_table = []
customers_table.append(['customer_id','year','revenue'])
customers_table.append(['int','int','int'])

pair_set = set()
for i in range(size):
    pair = generate_pair(id_pool, year_pool)
    while pair in pair_set:
        pair = generate_pair(id_pool, year_pool)
    pair_set.add(pair)
    customers_table.append([pair[0], pair[1], random.choice(rev_pool)])

# output tables
db_generator.output("Customers", customers_table)
