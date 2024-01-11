import random
from database_generator.database_generator import DatabaseGenerator, zipf_transform
from faker import Faker
import datetime
import sys

dbsize = sys.argv[1]

# some setups
db_generator = DatabaseGenerator(size =dbsize, script_file=__file__,
                                 problem_id="1141")

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
1141 schema

Table: Activity

+---------------+---------+
| Column Name   | Type    |
+---------------+---------+
| user_id       | int     |
| session_id    | int     |
| activity_date | date    |
| activity_type | enum    |
+---------------+---------+
There is no primary key for this table, it may have duplicate rows.
The activity_type column is an ENUM of type ('open_session', 'end_session', 'scroll_down', 'send_message').
The table shows the user activities for a social media website. 
Note that each session belongs to exactly one user.
"""

# setup columns
num_row = size
user_num = int(num_row / 10)
user_id_pool = range(user_num)
date_pool = [fake.date_between(datetime.date(2019,5,20), datetime.date(2019,7,30)) for _ in range(50)]
activity_type_pool = ['open_session', 'end_session', 'scroll_down', 'send_message']


user_id_pool = zipf_transform(user_id_pool)
date_pool = zipf_transform(date_pool)
activity_type_pool = zipf_transform(activity_type_pool)

user_id = [random.choice(user_id_pool) for _ in range(num_row)]
session_id = list(range(num_row))
activity_date = [random.choice(date_pool) for _ in range(num_row)]
activity_type = [random.choice(activity_type_pool) for _ in range(num_row)]

# prepare table

activity = list(zip(user_id, session_id, activity_date, activity_type))
activity.reverse()
activity.append(['int','int','date','str'])
activity.append(['user_id','session_id','activity_date','activity_type'])
activity.reverse()

# output tables
db_generator.output("Activity", activity)