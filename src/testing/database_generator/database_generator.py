import os
import csv

class DatabaseGenerator:
    def __init__(self, problem_id, base_path):
        problem_id = str(problem_id)
        self.problem_id = problem_id
        # self.script_file = script_file
        self.base_path = f"{base_path}/problem{self.problem_id}"
        os.makedirs(self.base_path, exist_ok=True)
        # self.output_dir = f"{self.base_path}/{inc}"

    def output(self, table_name: str, data, inc):
        table_name = table_name.lower()
        output_dir = f"{self.base_path}/{inc}"
        os.makedirs(output_dir, exist_ok=True)
        with open(output_dir + "/" + table_name + ".csv", "w") as f:
            writer = csv.writer(f)
            writer.writerows(data)

    def save_schema(self, schema):
        with open(f'{self.base_path}/schema.txt', 'w') as f:
            for tab_name in schema.keys():
                f.write(f'{tab_name}\n')
                for col_name in schema[tab_name]:
                    f.write(f'{col_name},')
                f.write('\n')
            # f.write(schema)


