import random
from database_generator.database_generator import DatabaseGenerator, generate_pair
import sys


dbsize = sys.argv[1]

# some setups
db_generator = DatabaseGenerator(size =dbsize, script_file=__file__,
                                 problem_id="596")

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
596 schema

+-------------+---------+
| Column Name | Type    |
+-------------+---------+
| student     | varchar |
| class       | varchar |
+-------------+---------+
(student, class) is the primary key column for this table.
Each row of this table indicates the name of a student and the class in which they are enrolled.

"""

# setup data pool]
class_size = int(size / 5)
student_size = 1000

class_pool = ['c_' + str(x) for x in range(class_size)]
student_pool = ['s_' + str(x) for x in range(student_size)]


#create courses table
table = []
table.append(['student','class'])
table.append(['str','str'])

pair_set = set()

for i in range(size):
    pair = generate_pair(student_pool, class_pool)
    while pair in pair_set:
        pair = generate_pair(student_pool, class_pool)
    pair_set.add(pair)
    table.append(list(pair))


# output tables
db_generator.output("Courses", table)
