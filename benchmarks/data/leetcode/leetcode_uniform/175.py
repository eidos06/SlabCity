import random
from database_generator.database_generator import DatabaseGenerator
import sys

dbsize = sys.argv[1]

# some setups
db_generator = DatabaseGenerator(size =dbsize, script_file=__file__,
                                 problem_id="175")

seed = 2333
random.seed(seed)

size = 1000000

if dbsize == '10M':
    size = 10000000
elif dbsize == '1M':
    size = 1000000
elif dbsize == '100K':
    size = 100000


name_num = int(size / 100)

pid_pool = [i for i in range(size)]
aid_pool = [i for i in range(size)]
first_name_pool = ['f_name_' + str(i) for i in range(name_num)]
last_name_pool = ['l_name_' + str(i) for i in range(name_num)]
city_pool = ['c_name_' + str(i) for i in range(500)]
state_pool = ['s_name_' + str(i) for i in range(50)]

person_table = []
person_table.append(['personid','firstname','lastname'])
person_table.append(['int','str','str'])
for pid in pid_pool:
    person_table.append([pid, random.choice(first_name_pool), random.choice(last_name_pool)])
address_table = []
address_table.append(['addressid','personid','city','state'])
address_table.append(['int','int','str','str'])
for aid in aid_pool:
    address_table.append([aid, random.choice(pid_pool), random.choice(city_pool), random.choice(state_pool)])
# output tables
db_generator.output("Person", person_table)
db_generator.output("Address", address_table)
