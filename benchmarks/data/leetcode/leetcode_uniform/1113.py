import random
from database_generator.database_generator import DatabaseGenerator
from faker import Faker
import datetime
import sys

dbsize = sys.argv[1]

# some setups
db_generator = DatabaseGenerator(size =dbsize, script_file=__file__,
                                 problem_id="1113")

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
1113 schema

Table: Actions

+---------------+---------+
| Column Name   | Type    |
+---------------+---------+
| user_id       | int     |
| post_id       | int     |
| action_date   | date    | 
| action        | enum    |
| extra         | varchar |
+---------------+---------+
There is no primary key for this table, it may have duplicate rows.
The action column is an ENUM type of ('view', 'like', 'reaction', 'comment', 'report', 'share').
The extra column has optional information about the action, such as a reason for the report or a type of reaction.

"""

# setup columns
num_row = size
id_pool = range(num_row)
post_id_pool = range(int(num_row / 100))
action_date_pool = [fake.date_between(datetime.date(2019,6,20), datetime.date(2019,7,5)) for _ in range(50)]
action_pool = ['view','like','reaction','comment','report','share']

user_id = [random.choice(id_pool) for _ in range(num_row)]
post_id = [random.choice(post_id_pool) for _ in range(num_row)]
action_date = [random.choice(action_date_pool) for _ in range(num_row)]
action = [random.choice(action_pool) for _ in range(num_row)]


extra_pool = list(range(10))
extra_pool = [str(x) for x in extra_pool]
extra = [random.choice(extra_pool) if action[i] == 'report' else '' for i in range(num_row)]

# prepare table

actions = list(zip(user_id, post_id, action_date, action, extra))
actions.reverse()
actions.append(['int','int','date','str','str'])
actions.append(['user_id','post_id','action_date','action','extra'])
actions.reverse()

# output tables
db_generator.output("Actions", actions)