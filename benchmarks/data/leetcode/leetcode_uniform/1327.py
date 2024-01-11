import random, datetime
from database_generator.database_generator import DatabaseGenerator
from faker import Faker
import sys

dbsize = sys.argv[1]

# some setups
db_generator = DatabaseGenerator(size =dbsize, script_file=__file__,
                                 problem_id="1327")

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


date_pool = [fake.date_between(datetime.date(2020,1,1), datetime.date(2020,12,31)) for _ in range(200)]


id_pool = [i for i in range(size)]
name_pool = ['name_' + str(i) for i in range(3000)]
category_pool = ['category_' + str(i) for i in range(100)]
unit_pool = [i for i in range(200)]


products_table = []
products_table.append(['product_id','product_name','product_category'])
products_table.append(['int','str','str'])
orders_table = []
orders_table.append(['product_id','order_date','unit'])
orders_table.append(['int','date','int'])

for ele in id_pool:
    products_table.append([ele, random.choice(name_pool), random.choice(category_pool)])

for i in range(size):
    orders_table.append([random.choice(id_pool), random.choice(date_pool), random.choice(unit_pool)])

# output tables
db_generator.output("Products", products_table)
db_generator.output("Orders", orders_table)
