from synthesizerv2.basics import *
from synthesizerv2.analysis import *
from synthesizerv2.generate_sketch import *
from synthesizerv2.concretize import *
from synthesizerv2.filter import *
from experiments.exp_basics import get_alpha_equiv_hash, get_query_hash, printProgressBar


def optimize(q: QueryWithSchema, allowed_wrap_level=1, err_file=None):
    # first make a deep copy to maintain invariant(optimize should not change the original q)
    q = deepcopy(q)
    result = []
    result_hash_list = set()
    # analysis
    mapping = analyze_base_table_and_columns(q)
    info_targetlist = analyze_target_list_chains(q.query, mapping)
    info_structure = analyze_table_structure(q, mapping)
    info_conditions = analyze_join_conditions(q.query, mapping)
    list_necessary_table_sets = analyze_necessary_table_sets(info_targetlist, info_conditions)
    info_filters = extract_filters(q, info_conditions, mapping)

    # print(len(info_filters))
    # for sublist in info_filters:
        # print(len(sublist))
        # for ele in sublist:
        #     print(ele.content)
        #     print(ele.scope)
    # print("======")

    # generate sketches
    sketches = generate_sketches(q.schema, info_structure, list_necessary_table_sets, max_wrap_level=allowed_wrap_level)
    # print("total number of generated sketches:")
    # print(len(sketches))
    # for s in sketches:
    #     print(IndentedStream()(s.query))

    current_dealt_sketches = 0
    for s in sketches:
        allocated_sketches = generate_annotated_sketch(s, info_filters)
        # print(len(allocated_sketches))
        # count += len(allocated_sketches)
        # print(count)

        # if current_dealt_sketches > 3:
        #     break

        for allocated_sketch in allocated_sketches:
            try:
                # if -1 not in allocated_sketch.filter_indicator:
                    # for complete_sketch in concretize(allocated_sketch, q.schema, info_targetlist):
                    #     yield complete_sketch
                complete_sketches = concretize(allocated_sketch, q.schema, info_targetlist)
                tmp_deduplicated_results = []
                for candidate_result in complete_sketches:
                    query_hash = get_alpha_equiv_hash(candidate_result)
                    if query_hash in result_hash_list:
                        continue
                    else:
                        tmp_deduplicated_results.append(candidate_result)
                        result_hash_list.add(query_hash)
                yield tmp_deduplicated_results

            except Exception as e:
                if err_file is not None:
                    err_file.write(str(e) + "\n")
                else:
                    print(e)
                pass
        current_dealt_sketches += 1
        # printProgressBar(current_dealt_sketches, len(sketches), "current dealt sketches:", str(current_dealt_sketches) + "/" + str(len(sketches)))
