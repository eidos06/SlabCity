import random
from database_generator.database_generator import DatabaseGenerator, generate_pair, zipf_transform
import sys

dbsize = sys.argv[1]

# some setups
db_generator = DatabaseGenerator(size =dbsize, script_file=__file__,
                                 problem_id="1069")

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
sale
+-------------+-------+
| Column Name | Type  |
+-------------+-------+
| sale_id     | int   |
| product_id  | int   |
| year        | int   |
| quantity    | int   |
| price       | int   |
+-------------+-------+
(sale_id, year) is the primary key of this table.
product_id is a foreign key to Product table.
Each row of this table shows a sale on the product product_id in a certain year.
Note that the price is per unit.

product
+--------------+---------+
| Column Name  | Type    |
+--------------+---------+
| product_id   | int     |
| product_name | varchar |
+--------------+---------+
product_id is the primary key of this table.
Each row of this table indicates the product name of each product.
"""

# setup columns
product_num = int(size / 100)
sale_id_num = int(size / 5)
year_num = 20


product_id = list(range(product_num))
product_name_pool = ['p_name_' + str(i) for i in range(int(product_num/2))]

sale_id_pool = list(range(sale_id_num))
year_pool = list(range(2000,2020 + year_num))
quantity_pool = list(range(10, 20))
price_pool = list(range(10,20))

transformed_product_id = zipf_transform(product_id)
sale_id_num = zipf_transform(sale_id_pool)
year_pool = zipf_transform(year_pool)
quantity_pool = zipf_transform(quantity_pool)
price_pool = zipf_transform(price_pool)
product_name_pool = zipf_transform(product_name_pool)

# fill in columns

sales = []
sales.append(['sale_id','year','product_id','quantity','price'])
sales.append(['int', 'int', 'int','int','int'])
pair_set = set()

for i in range(size):
    pair = generate_pair(sale_id_pool, year_pool)
    while pair in pair_set:
        pair = generate_pair(sale_id_pool, year_pool)
    pair_set.add(pair)
    sales.append([pair[0], pair[1], random.choice(transformed_product_id), random.choice(quantity_pool), random.choice(price_pool)])

product = []
product.append(['product_id','product_name'])
product.append(['int', 'str'])
for i in range(product_num):
    product.append([product_id[i], random.choice(product_name_pool)])

# output tables
db_generator.output("Sales", sales)
db_generator.output("Product", product)
