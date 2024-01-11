import networkx as nx
import random
from database_generator.database_generator import DatabaseGenerator
import sys


dbsize = sys.argv[1]

# some setups
db_generator = DatabaseGenerator(size =dbsize, script_file=__file__,
                                 problem_id="614")

size = 1000000

if dbsize == '10M':
    size = 10000000
elif dbsize == '1M':
    size = 1000000
elif dbsize == '100K':
    size = 100000

G = nx.barabasi_albert_graph(int(size/2), 2, seed=2333)
edge_list = list(G.edges)
edge_list = [list(x) for x in edge_list]
random.shuffle(edge_list)


"""
+-------------+---------+
| Column Name | Type    |
+-------------+---------+
| followee    | varchar |
| follower    | varchar |
+-------------+---------+
(followee, follower) is the primary key column for this table.
Each row of this table indicates that the user follower follows the user followee on a social network.
There will not be a user following themself.
"""

# setup columns
# size = 1000000

# fill in columns

table = []
table.append(['followee', 'follower'])
table.append(['str', 'str'])

for e in edge_list:
    e = [str(x) for x in e]
    table.append(e)


# output tables
db_generator.output("Follow", table)