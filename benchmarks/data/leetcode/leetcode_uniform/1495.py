import random
from database_generator.database_generator import DatabaseGenerator, generate_pair
from faker import Faker
import datetime
import sys

dbsize = sys.argv[1]

# some setups
db_generator = DatabaseGenerator(size =dbsize, script_file=__file__,
                                 problem_id="1495")

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
1495 schema

Table: TVProgram

+---------------+---------+
| Column Name   | Type    |
+---------------+---------+
| program_date  | date    |
| content_id    | int     |
| channel       | varchar |
+---------------+---------+
(program_date, content_id) is the primary key for this table.
This table contains information of the programs on the TV.
content_id is the id of the program in some channel on the TV.
 

Table: Content

+------------------+---------+
| Column Name      | Type    |
+------------------+---------+
| content_id       | varchar |
| title            | varchar |
| Kids_content     | enum    |
| content_type     | varchar |
+------------------+---------+
content_id is the primary key for this table.
Kids_content is an enum that takes one of the values ('Y', 'N') where: 
'Y' means is content for kids otherwise 'N' is not content for kids.
content_type is the category of the content as movies, series, etc.

"""

# setup columns
content_num = int(size / 10)

date_pool = [fake.date_between(datetime.date(2020,1,1),datetime.date(2020,12,31)) for _ in range(200)]
content_id_pool = list(range(content_num))
channel_pool = ['c' + str(i) for i in range(300)]

title_pool = ['title_' + str(i) for i in range(int(content_num / 20))]
kids_pool = ['Y', 'N']
content_type_pool = ["Movies", "Series", "Songs"]


# fill in columns
tvprogram = []
tvprogram.append(['program_date','content_id','channel'])
tvprogram.append(['date','str','str'])

pair_set = set()
for i in range(size):
    pair = generate_pair(date_pool, content_id_pool)
    while pair in pair_set:
        pair = generate_pair(date_pool, content_id_pool)
    pair_set.add(pair)
    tvprogram.append([pair[0], pair[1], random.choice(channel_pool)])

content = []
content.append(['content_id','title','kids_content','content_type'])
content.append(['str','str','str','str'])
for i in range(content_num):
    content.append([str(content_id_pool[i]), random.choice(title_pool), random.choice(kids_pool), random.choice(content_type_pool)])

# output tables
db_generator.output("TVProgram", tvprogram)
db_generator.output("Content", content)
