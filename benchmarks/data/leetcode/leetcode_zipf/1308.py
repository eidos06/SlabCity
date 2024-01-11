import random
from database_generator.database_generator import DatabaseGenerator, generate_pair, zipf_transform
import datetime
import sys

dbsize = sys.argv[1]

# some setups
db_generator = DatabaseGenerator(size =dbsize, script_file=__file__,
                                 problem_id="1308")

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
1308 schema

Table: Scores

+---------------+---------+
| Column Name   | Type    |
+---------------+---------+
| player_name   | varchar |
| gender        | varchar |
| day           | date    |
| score_points  | int     |
+---------------+---------+
(gender, day) is the primary key for this table.
A competition is held between the female team and the male team.
Each row of this table indicates that a player_name and with gender has scored score_point in someday.
Gender is 'F' if the player is in the female team and 'M' if the player is in the male team.

"""

# setup columns
num_row = size
player_num = 5000
day_num = 30000
gender_num = int(size * 9 / day_num)

player_name_pool = ['name_' + str(i) for i in range(player_num)]
gender = ['gender' + str(i) for i in range(gender_num)]

base = datetime.date(2200,1,1)
day_pool = [base - datetime.timedelta(days=x) for x in range(day_num)]
# day_pool = [fake.date_between(datetime.date(1000,1,1), datetime.date(5000,1,1)) for i in range(day_num)]
score_points = [random.randint(1, 15) for i in range(num_row)]

player_name_pool = zipf_transform(player_name_pool)
gender = zipf_transform(gender)
day_pool = zipf_transform(day_pool)

# fill in columns

table = []
table.append(['gender','day','player_name','score_points'])
table.append(['str','date','str','int'])
pair_set = set()

for i in range(num_row):
    pair = generate_pair(gender, day_pool)
    while pair in pair_set:
        pair = generate_pair(gender, day_pool)
    pair_set.add(pair)
    table.append([pair[0], pair[1], random.choice(player_name_pool), random.choice(score_points)])

# output tables
db_generator.output("Scores", table)
