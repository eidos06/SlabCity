import os
import csv
import numpy as np
import math
import random

class DatabaseGenerator:
    def __init__(self, size, problem_id:str|int, script_file):
        problem_id = str(problem_id)
        self.problem_id = problem_id
        self.script_file = script_file
        self.output_dir = "output"

    def output(self, table_name: str, data):
        table_name = table_name
        os.makedirs(self.output_dir, exist_ok=True)
        with open(self.output_dir + "/" + table_name + ".csv", "w") as f:
            writer = csv.writer(f)
            writer.writerows(data)

    def finish(self):
        with open(self.output_dir + "/" + "script.py", "w") as f:
            with open(self.script_file) as s:
                f.write(s.read())



def zipf_transform(pool):
    np.random.seed(2333)
    transformed_pool = []
    dist = np.random.zipf(1.25, len(pool))
    while np.sum(dist) < 0:
        # generate a random number as seed
        random.seed()
        np.random.seed(random.randrange(100))
        dist = np.random.zipf(1.25, len(pool))

    dist = dist / np.sum(dist)
    for i in range(len(pool)):
        frequency = math.ceil(dist[i] * len(pool))
        transformed_pool.extend([pool[i]] * frequency)

    return transformed_pool

def generate_pair(pool1, pool2):
    p1 = random.choice(pool1)
    p2 = random.choice(pool2)
    return (p1, p2)