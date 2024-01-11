import random
from database_generator.database_generator import DatabaseGenerator, generate_pair
from faker import Faker
import datetime
import sys


dbsize = sys.argv[1]

# some setups
db_generator = DatabaseGenerator(size =dbsize, script_file=__file__,
                                 problem_id="534")

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
+--------------+---------+
| Column Name  | Type    |
+--------------+---------+
| player_id    | int     |
| device_id    | int     |
| event_date   | date    |
| games_played | int     |
+--------------+---------+
(player_id, event_date) is the primary key of this table.
This table shows the activity of players of some games.
Each row is a record of a player who logged in and played a number of games (possibly 0) before logging out on someday using some device.
"""

# setup columns
num_row = size

player_num = int(num_row / 200)

player_id_pool = list(range(player_num))
device_id_pool = list(range(20))
event_date_pool = [fake.unique.date_between(datetime.date(2009,1,1), datetime.date(2022,11,30)) for _ in range(1000)]
games_played_pool = list(range(1,10))

# fill in columns

table = []
table.append(['player_id','event_date','device_id','games_played'])
table.append(['int', 'date','int','int'])

pair_set = set()

for i in range(num_row):
    pair = generate_pair(player_id_pool, event_date_pool)
    while pair in pair_set:
        pair = generate_pair(player_id_pool, event_date_pool)
    pair_set.add(pair)
    table.append([pair[0], pair[1], random.choice(device_id_pool), random.choice(games_played_pool)])

# output tables
db_generator.output("Activity", table)
