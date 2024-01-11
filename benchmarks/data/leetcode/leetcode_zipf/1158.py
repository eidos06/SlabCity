import random
from database_generator.database_generator import DatabaseGenerator, zipf_transform
from faker import Faker
import datetime
import sys

dbsize = sys.argv[1]

# some setups
db_generator = DatabaseGenerator(size =dbsize, script_file=__file__,
                                 problem_id="1158")

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
1158 schema

Table: Users

+----------------+---------+
| Column Name    | Type    |
+----------------+---------+
| user_id        | int     |
| join_date      | date    |
| favorite_brand | varchar |
+----------------+---------+
user_id is the primary key of this table.
This table has the info of the users of an online shopping website where users can sell and buy items.

Table: Orders

+---------------+---------+
| Column Name   | Type    |
+---------------+---------+
| order_id      | int     |
| order_date    | date    |
| item_id       | int     |
| buyer_id      | int     |
| seller_id     | int     |
+---------------+---------+
order_id is the primary key of this table.
item_id is a foreign key to the Items table.
buyer_id and seller_id are foreign keys to the Users table.

Table: Items

+---------------+---------+
| Column Name   | Type    |
+---------------+---------+
| item_id       | int     |
| item_brand    | varchar |
+---------------+---------+
item_id is the primary key of this table.

"""

# setup columns
total_user = size
total_order = size
total_item = size
date_pool = [fake.date_between(datetime.date(2014,1,1),datetime.date(2022,12,31)) for _ in range(2000)]
user_id_pool = list(range(size))
brand_pool = ['brand_' + str(i) for i in range(10000)]
order_id_pool = list(range(size))
item_id_pool = list(range(size))

# fill in columns

user_id = user_id_pool

transformed_user_id_pool = zipf_transform(user_id_pool)
transformed_item_id_pool = zipf_transform(item_id_pool)
date_pool = zipf_transform(date_pool)
brand_pool = zipf_transform(brand_pool)

join_date = random.choices(date_pool, k=total_user)
favorite_brand = random.choices(brand_pool, k=total_user)

order_id = order_id_pool
order_date = random.choices(date_pool,k = total_order)
item_id = random.choices(transformed_item_id_pool, k = total_order)
buyer_id = random.choices(transformed_user_id_pool, k = total_order)
seller_id = random.choices(transformed_user_id_pool, k = total_order)

item_id_pool = item_id_pool
item_brand = random.choices(brand_pool, k = total_item)

# prepare table

users = list(zip(user_id, join_date, favorite_brand))
users.reverse()
users.append(['int','date','str'])
users.append(['user_id','join_date','favorite_brand'])
users.reverse()

orders = list(zip(order_id, item_id, buyer_id, seller_id, order_date))
orders.reverse()
orders.append(['int','int','int','int','date'])
orders.append(['order_id','item_id','buyer_id','seller_id','order_date'])
orders.reverse()

items = list(zip(item_id_pool, item_brand))
items.reverse()
items.append(['int', 'str'])
items.append(['item_id', 'item_brand'])
items.reverse()

# output tables
db_generator.output("Users", users)
db_generator.output("Orders", orders)
db_generator.output("Items", items)
