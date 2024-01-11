import random

from database_generator.database_generator import DatabaseGenerator
from faker import Faker
import sys

dbsize = sys.argv[1]
# some setups
db_generator = DatabaseGenerator(size = dbsize, script_file=__file__,
                                 problem_id="176")
seed = 2333
Faker.seed(seed)
fake = Faker()
random.seed(seed)

# start logic writing
size = 1000000

if dbsize == '10M':
    size = 10000000
elif dbsize == '1M':
    size = 1000000
elif dbsize == '100K':
    size = 100000

employee_id_pool = list(range(size))
salary_pool = [i for i in range(500,5000)]

employee_table = list(zip(employee_id_pool, random.choices(salary_pool, k=size)))
employee_table.reverse()
employee_table.append(['int', 'int'])
employee_table.append(['id', 'salary'])
employee_table.reverse()

db_generator.output("Employee", employee_table)