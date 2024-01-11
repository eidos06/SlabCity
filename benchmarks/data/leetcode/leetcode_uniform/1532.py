import random
from database_generator.database_generator import DatabaseGenerator, generate_pair
from faker import Faker
import datetime
import sys

dbsize = sys.argv[1]

# some setups
db_generator = DatabaseGenerator(size =dbsize, script_file=__file__,
                                 problem_id="1532")

# setup seed
seed = 2333
Faker.seed(seed)
fake = Faker()
random.seed(seed)

size = 1000000

if dbsize == '10M':
    size = 10000000
elif dbsize == '1M':
    size = 1000000
elif dbsize == '100K':
    size = 100000

"""
1532 schema

Table: Customers

+---------------+---------+
| Column Name   | Type    |
+---------------+---------+
| customer_id   | int     |
| name          | varchar |
+---------------+---------+
customer_id is the primary key for this table.
This table contains information about customers.
 

Table: Orders

+---------------+---------+
| Column Name   | Type    |
+---------------+---------+
| order_id      | int     |
| order_date    | date    |
| customer_id   | int     |
| cost          | int     |
+---------------+---------+
order_id is the primary key for this table.
This table contains information about the orders made by customer_id.
Each customer has one order per day.
"""

# setup columns

total = size
customer_num = int(total / 3)

order_id = list(range(total))
customer_id_pool = list(range(customer_num))

customer_name_pool = ['name_' + str(x) for x in range(int(customer_num / 4))]
date_pool = [fake.date_between(datetime.date(2020,1,1), datetime.date(2020,12,30)) for _ in range(200)]
cost_pool = list(range(1, 6))


# fill in columns

customer_name = [random.choice(customer_name_pool) for _ in range(customer_num)]

orders_table = []
orders_table.append(['order_id','order_date','customer_id','cost'])
orders_table.append(['int','date','int','int'])

pair_set = set()

for i in range(total):
    pair = generate_pair(date_pool, customer_id_pool)
    while pair in pair_set:
        pair = generate_pair(date_pool, customer_id_pool)
    pair_set.add(pair)
    orders_table.append([order_id[i], pair[0], pair[1], random.choice(cost_pool)])

# prepare table

customers_table = list(zip(customer_id_pool, customer_name))
customers_table.reverse()
customers_table.append(['int','str'])
customers_table.append(['customer_id', 'name'])
customers_table.reverse()

# output tables
db_generator.output("Customers", customers_table)
db_generator.output("Orders", orders_table)