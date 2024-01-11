import random
from database_generator.database_generator import DatabaseGenerator, zipf_transform
import sys

dbsize = sys.argv[1]

# some setups
db_generator = DatabaseGenerator(size =dbsize, script_file=__file__,
                                 problem_id="1677")

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

product_num = int(size / 10)

pid_pool = list(range(product_num))
name_pool = ['pname_' + str(i) for i in range(product_num)]

invoice_id = list(range(size))
num_pool = [i for i in range(10)]

transformed_pid_pool = zipf_transform(pid_pool)
num_pool = zipf_transform(num_pool)
name_pool = zipf_transform(name_pool)

invoice = []
invoice.append(['invoice_id','product_id','rest','paid','canceled','refunded'])
invoice.append(['int', 'int','int','int','int','int'])

product = []
product.append(['product_id','name'])
product.append(['int', 'str'])

for i in range(product_num):
    product.append([pid_pool[i], name_pool[i]])

for i in range(size):
    invoice.append([invoice_id[i], random.choice(transformed_pid_pool), random.choice(num_pool), random.choice(num_pool), random.choice(num_pool), random.choice(num_pool)])

# output tables
db_generator.output("Product", product)
db_generator.output("Invoice", invoice)
