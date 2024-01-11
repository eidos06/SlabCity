import random
from database_generator.database_generator import DatabaseGenerator
from faker import Faker
import datetime
import sys

dbsize = sys.argv[1]

# some setups
db_generator = DatabaseGenerator(size =dbsize, script_file=__file__,
                                 problem_id="1693")

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
1693 schema

Table: DailySales

+-------------+---------+
| Column Name | Type    |
+-------------+---------+
| date_id     | date    |
| make_name   | varchar |
| lead_id     | int     |
| partner_id  | int     |
+-------------+---------+
This table does not have a primary key.
This table contains the date and the name of the product sold and the IDs of the lead and partner it was sold to.
The name consists of only lowercase English letters.

"""

# setup columns
total = size

date_num = 1000
make_name_num = int(size / 2 / date_num)
id_num = 20


date_pool = [fake.date_between(datetime.date(2010,1,1), datetime.date(2019,1,6)) for i in range(date_num)]
make_name_pool = ['make_' + str(i) for i in range(make_name_num)]
id_pool = [i for i in range(id_num)]

# fill in columns
date = [random.choice(date_pool) for i in range(total)]
name = [random.choice(make_name_pool) for i in range(total)]
lead = [random.choice(id_pool) for i in range(total)]
partner = [random.choice(id_pool) for i in range(total)]

# prepare table

table = list(zip(date,name,lead,partner))
table.reverse()
table.append(['date', 'str','int','int'])
table.append(['date_id','make_name','lead_id','partner_id'])
table.reverse()
# output tables
db_generator.output("DailySales", table)
