from structs import ResultSet
from structs import timeout
import structs
from csv import reader


def main():

    workloads = [
        'LeetCode',
        'Calcite',
    ]
    workload_to_datasizes = {
        'LeetCode': ['100K', '1M', '10M'],
        'Calcite': ['250K', '1M', '4M'],
    }

    workload_datasize_to_raw_file_paths = {
        # Calcite
        ('Calcite', '250K'): "../calcite-250K/raw.csv",
        ('Calcite', '1M'): "../calcite-1M/raw.csv",
        ('Calcite', '4M'): "../calcite-4M/raw.csv",
        # LeetCode
        ('LeetCode', '100K'): "../leetcode-100K/raw.csv",
        ('LeetCode', '1M'): "../leetcode-1M/raw.csv",
        ('LeetCode', '10M'): "../leetcode-10M/raw.csv",
    }

    # read raw files
    result_sets = read_raw_files(
        workloads=workloads,
        workload_to_datasizes=workload_to_datasizes,
        workload_datasize_to_raw_file_paths=workload_datasize_to_raw_file_paths
    )

    # sanity check
    for result_set in result_sets:
        result_set.sanity_check()

    for result_set in result_sets:
        workload = result_set.get_workload()
          
        result_set.paper_plot_coverage()
        result_set.paper_plot_perf()

def read_raw_files(workloads, workload_to_datasizes, workload_datasize_to_raw_file_paths):
    ret = []
    for workload in workloads:
        result_set = ResultSet(workload=workload)
        for datasize in workload_to_datasizes[workload]:
            raw_file = workload_datasize_to_raw_file_paths[(
                workload, datasize)]
            print("Reading.. " + raw_file)
            # read input query results..
            removed = {}
            with open(raw_file, 'r') as read_obj:
                next(read_obj)
                csv_reader = reader(read_obj)
                for row in csv_reader:
                    pid = row[0]
                    qid = row[1].split("-")[1]
                    query = row[2]
                    cost = row[3]
                    time = timeout[workload] if row[4] == 'timeout' else row[4]
                    assert float(time) > 0, row
                    if row[1].split("-")[0] == 'input':
                        if float(cost) == 0:
                            removed[(pid, qid)] = row
                            print("Removed one input query..")
                            continue
                        result_set.add_input_measure(
                            datasize=datasize,
                            problem_id=pid,
                            query_id=qid,
                            measure='cost',
                            value=cost,
                            query=query
                        )
                        result_set.add_input_measure(
                            datasize=datasize,
                            problem_id=pid,
                            query_id=qid,
                            measure='time',
                            value=time,
                            query=query
                        )
            # read output query results for all tools..
            with open(raw_file, 'r') as read_obj:
                next(read_obj)
                csv_reader = reader(read_obj)
                for row in csv_reader:
                    pid = row[0]
                    qid = row[1].split("-")[1]
                    query = row[2]
                    cost = row[3]
                    time = timeout[workload] if row[4] == 'timeout' else row[4]
                    assert float(time) > 0, row
                    tool = row[1].split("-")[0]
                    if row[1].split("-")[0] != 'input':
                        assert tool in ['SC', 'LR', 'WT']
                        if (pid, qid) in removed.keys():
                            row = removed[(pid, qid)]
                            assert float(row[3]) == 0
                            continue

                        result_set.add_tool_measure(
                            datasize=datasize,
                            problem_id=pid,
                            query_id=qid,
                            tool=tool,
                            measure='cost',
                            value=cost,
                            query=query
                        )
                        result_set.add_tool_measure(
                            datasize=datasize,
                            problem_id=pid,
                            query_id=qid,
                            tool=tool,
                            measure='time',
                            value=time,
                            query=query
                        )
        result_set.set_GT_results()
        ret.append(result_set)
    return ret

if __name__ == "__main__":
    main()

