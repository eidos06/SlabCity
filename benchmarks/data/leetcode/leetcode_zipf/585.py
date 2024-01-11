import random
from database_generator.database_generator import DatabaseGenerator, zipf_transform
import sys


dbsize = sys.argv[1]

# some setups
db_generator = DatabaseGenerator(size =dbsize, script_file=__file__,
                                 problem_id="585")

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


# start logic writing

id_pool = [i for i in range(size)]
amount_pool = [i for i in range(20, 30)]
lat_pool = [float(i) for i in range(50)]

amount_pool = zipf_transform(amount_pool)
lat_pool = zipf_transform(lat_pool)

insurance_table = []
insurance_table.append(['pid','tiv_2015','tiv_2016','lat','lon'])
insurance_table.append(['int','numeric','numeric','numeric','numeric'])

for id in id_pool:
    this_lat = random.choice(lat_pool)
    insurance_table.append([id, random.choice(amount_pool), random.choice(amount_pool), this_lat, this_lat])


# output tables
db_generator.output("Insurance", insurance_table)
