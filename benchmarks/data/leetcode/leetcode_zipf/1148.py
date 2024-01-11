import random
from database_generator.database_generator import DatabaseGenerator, zipf_transform
from faker import Faker
import datetime
import sys

dbsize = sys.argv[1]

# some setups
db_generator = DatabaseGenerator(size =dbsize, script_file=__file__,
                                 problem_id="1148")

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
1148 schema

Table: Views

+---------------+---------+
| Column Name   | Type    |
+---------------+---------+
| article_id    | int     |
| author_id     | int     |
| viewer_id     | int     |
| view_date     | date    |
+---------------+---------+
There is no primary key for this table, it may have duplicate rows.
Each row of this table indicates that some viewer viewed an article (written by some author) on some date. 
Note that equal author_id and viewer_id indicate the same person.
"""

# setup columns

total = size
# total / (num_member ^ 2) = 1
num_member = int(total ** 0.5)

artical_id_pool = list(range(10000))
member_id_pool = list(range(num_member))
view_date_pool = [fake.date_between(datetime.date(2015,1,1), datetime.date(2021,12,30)) for _ in range(500)]


artical_id_pool = zipf_transform(artical_id_pool)
member_id_pool = zipf_transform(member_id_pool)
view_date_pool = zipf_transform(view_date_pool)

# fill in columns
article_id = [random.choice(artical_id_pool) for _ in range(total)]
author_id = [random.choice(member_id_pool) for _ in range(total)]
viewer_id = [random.choice(member_id_pool) for _ in range(total)]
view_date = [random.choice(view_date_pool) for _ in range(total)]


# prepare table
table = list(zip(article_id, author_id, viewer_id, view_date))
table.reverse()
table.append(['int','int', 'int', 'date'])
table.append(['article_id', 'author_id', 'viewer_id', 'view_date'])
table.reverse()
# output tables
db_generator.output("Views", table)