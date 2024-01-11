import random
from database_generator.database_generator import DatabaseGenerator, generate_pair, zipf_transform

import sys

dbsize = sys.argv[1]

# some setups
db_generator = DatabaseGenerator(size =dbsize, script_file=__file__,
                                 problem_id="1068")

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
product_id_pool = [i for i in range(size)]
year_pool = [i for i in range(2000, 2022)]
quantity_pool = [i for i in range(1,10)]
price_pool = [i for i in range(1, 20)]

product_name_pool = ['pname_' + str(i) for i in range(int(size / 100))]
sale_pool = list(range(int(size / 10)))

product_name_pool = zipf_transform(product_name_pool)
sale_pool = zipf_transform(sale_pool)
year_pool = zipf_transform(year_pool)
transformed_product_id_pool = zipf_transform(product_id_pool)
quantity_pool = zipf_transform(quantity_pool)
price_pool = zipf_transform(price_pool)

product_table = []
product_table.append(['product_id','product_name'])
product_table.append(['int','str'])
for i in product_id_pool:
    product_table.append([i, random.choice(product_name_pool)])

sales_table = []
sales_table.append(['sale_id','year','product_id','quantity','price'])
sales_table.append(['int','int','int','int','int'])

pair_set = set()

for i in range(size):
    pair = generate_pair(sale_pool, year_pool)
    while pair in pair_set:
        pair = generate_pair(sale_pool, year_pool)
    pair_set.add(pair)
    sales_table.append([pair[0], pair[1], random.choice(transformed_product_id_pool), random.choice(quantity_pool), random.choice(price_pool)])


# output tables
db_generator.output("Sales", sales_table)
db_generator.output("Product", product_table)
