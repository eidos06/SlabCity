from synthesizerv2.hole import *
from synthesizerv2.filter import *
from synthesizerv2.sketch import *
from synthesizerv2.analysis import JoinStructureInfo
from pglast.enums import *
from typing import Set
import itertools
from synthesizerv2.basics import *
from pglast.stream import IndentedStream

class AbstractStructure:
    used_join_structure_bit_vector: int
    used_table_bit_vector: int

    def __init__(self, used_join_structure_bit_vector: int = 0, used_table_bit_vector: int = 0):
        self.used_join_structure_bit_vector = used_join_structure_bit_vector
        self.used_table_bit_vector = used_table_bit_vector
        pass

    def __str__(self):
        pass


class AbstractTableStructure(AbstractStructure):
    table_name: str

    def __init__(self, table_name, used_join_structure_bit_vector: int = 0, used_table_bit_vector: int = 0):
        super().__init__(used_join_structure_bit_vector, used_table_bit_vector)
        self.table_name = table_name

    def __str__(self):
        return self.table_name


class AbstractJoinStructure(AbstractStructure):
    larg: AbstractStructure
    rarg: AbstractStructure
    jointype: nodes.JoinType
    quals_scope_bit_vec: int

    def __init__(self, larg, rarg, used_join_structure_bit_vector: int = 0, used_table_bit_vector: int = 0, jointype: nodes.JoinType = JoinType.JOIN_INNER, quals_scope_bit_vec: int = 0):
        super().__init__(used_join_structure_bit_vector, used_table_bit_vector)
        self.larg = larg
        self.rarg = rarg
        self.jointype = jointype
        self.quals_scope_bit_vec = quals_scope_bit_vec

    def __str__(self):
        return "(" + str(self.larg) + " " + str(self.jointype) + " " + str(self.rarg) + ")"


def generate_abstract_sketches(tables: List[str], exist_join_structures: List[JoinStructureInfo],
                               required_table_possibilities: List[Set[str]]) -> List[AbstractStructure]:
                               
    def _can_join(candidate_1: AbstractStructure, candidate_2: AbstractStructure) -> List[int]:
        # a list of join structure that we can join for these two candidates
        result = []
        # we first need to make sure they don't use overlapped join info
        if candidate_1.used_join_structure_bit_vector & candidate_2.used_join_structure_bit_vector != 0:
            return []
        # then we need to see if there is a join structure we can use
        # first get all join structures of candidate 1 and candidate 2
        used_join_structure = candidate_1.used_join_structure_bit_vector | candidate_2.used_join_structure_bit_vector
        # then we loop all join structures
        for idx_j, j in enumerate(exist_join_structures):
            # if the join structure is used, then continue
            if used_join_structure & (1 << idx_j):
                continue
            # if the join structure is not used, and candidate1 and candiate 2 fulfills the join requirement, then add
            # it to the list
            if candidate_1.used_table_bit_vector & (1 << j.ltable) and candidate_2.used_table_bit_vector & (1 << j.rtable):
                result.append(idx_j)
        return result

    def _generate_helper_given_two_worklist(worklist_1, worklist_2) -> List[AbstractStructure]:
        result = []
        for idx_x, candidate_1 in enumerate(worklist_1):
            for idx_y, candidate_2 in enumerate(worklist_2):
                # if worklist_1 is worklist_2:
                #     if idx_y < idx_x:
                #         continue
                join_idx_list = _can_join(candidate_1, candidate_2)
                for join_index in join_idx_list:
                    new_used_table = candidate_1.used_table_bit_vector | candidate_2.used_table_bit_vector | \
                                     1 << exist_join_structures[join_index].ltable | \
                                     1 << exist_join_structures[join_index].rtable
                    new_join_structures = candidate_1.used_join_structure_bit_vector | candidate_2.used_join_structure_bit_vector \
                                          | 1 << join_index
                    # there are two cases:
                    # if (candidate_1.used_table_bit_vector & (1 << exist_join_structures[join_index].ltable) \
                    #         and candidate_2.used_table_bit_vector & (1 << exist_join_structures[join_index].rtable))\
                    #         or \
                    #         (candidate_1.used_table_bit_vector & (1 << exist_join_structures[join_index].rtable) \
                    #         and candidate_2.used_table_bit_vector & (1 << exist_join_structures[join_index].ltable)):
                    result.append(
                            AbstractJoinStructure(candidate_1, candidate_2, new_join_structures, new_used_table, exist_join_structures[join_index].join_type, 1 << exist_join_structures[join_index].ltable | \
                                     1 << exist_join_structures[join_index].rtable))

        return result

    # first: convert exist join structures to bit vectors
    exist_join_structures = deepcopy(exist_join_structures)
    for join_structure in exist_join_structures:
        join_structure.ltable = tables.index(join_structure.ltable)
        join_structure.rtable = tables.index(join_structure.rtable)

    # saturation worklist algorithm
    old_worklist: List[AbstractStructure] = []
    newly_added_worklist: List[AbstractStructure] = []
    # init newly added worklist
    # base case: initialize saturation worklist with a list of tables
    for idx, table in enumerate(tables):
        newly_added_worklist.append(AbstractTableStructure(table, 0, 1 << idx))

    while True:
        added_this_time: List[AbstractStructure] = []
        # test among newly_added and old_worklist to see if they can join
        added_this_time += _generate_helper_given_two_worklist(newly_added_worklist, old_worklist)
        # test among newly_added and newly_added to see if they can join
        added_this_time += _generate_helper_given_two_worklist(newly_added_worklist, newly_added_worklist)
        old_worklist = old_worklist + newly_added_worklist
        newly_added_worklist = added_this_time
        if len(newly_added_worklist) == 0:
            break
    # filter result using required table possibilities
    # first translate them into bit vector representation
    required_table_possibilities_bit_vec = []
    for i in required_table_possibilities:
        temp = 0
        for j in i:
            temp |= 1 << tables.index(j)
        required_table_possibilities_bit_vec.append(temp)
    # then filter
    after_filter = []
    for candidate in old_worklist:
        if len(required_table_possibilities_bit_vec) == 0:
            after_filter.append(candidate)
            continue
        for bit_vec in required_table_possibilities_bit_vec:
            if candidate.used_table_bit_vector & bit_vec == bit_vec:
                after_filter.append(candidate)
                break

    # deduplicate the result
    deduplicate_set = set()
    result_after_deduplicate = []
    for candidate in after_filter:
        string_representation = str(candidate)
        if string_representation not in deduplicate_set:
            deduplicate_set.add(string_representation)
            result_after_deduplicate.append(candidate)
    return result_after_deduplicate


def convert_abstract_structure_to_IR(abstract_structure: AbstractStructure, tables: List[str],
                                     max_wrap_level: int = 2, min_wrap_level: int = 1) -> \
        List[Node]:
    def convert_table_bitvec_to_table_scope(bitvec: int) -> Set[str]:
        result: Set[str] = set()
        for idx, table in enumerate(tables):
            if bitvec & (1 << idx):
                result.add(table)
        return result

    # wrap node with select statement
    def __wrapper(node_to_be_wrapped: Node, scope_set_for_wrapper, level_left) -> Node:
        if level_left == 0:
            return node_to_be_wrapped
        from_clause = __wrapper(node_to_be_wrapped, scope_set_for_wrapper, level_left - 1)
        target_list_hole = Hole_Target_List_With_Agg(0, scope_set_for_wrapper)
        renaming_list_hole = Hole_Rename_Renaming_List(0)
        where_hole = Hole_Where_Predicate(0, scope_set_for_wrapper)
        having_hole = Hole_Having_Predicate(0, scope_set_for_wrapper)
        groupby_hole = Hole_Column_List(0, scope_set_for_wrapper)
        wrapped_select_statement = Rename(renaming_list_hole, RangeSubselect(
            subquery=SelectStmt(targetList=target_list_hole, fromClause=tuple([from_clause]), whereClause=where_hole,
                                groupClause=groupby_hole, havingClause=having_hole)))
        return wrapped_select_statement

    def __extend_helper(abstract_structure: AbstractStructure, _max_wrap_level: int, _min_wrap_level:int) -> List[Node]:
        if isinstance(abstract_structure, AbstractTableStructure):
            result = []
            scope_set = convert_table_bitvec_to_table_scope(abstract_structure.used_table_bit_vector)
            rename_renaming_list = Hole_Rename_Renaming_List(0,scope_set)
            node_in_IR = Rename(rename_renaming_list, RangeVar(relname=abstract_structure.table_name, inh=True))
            for i in range(_min_wrap_level, _max_wrap_level+1):
                scope_set = convert_table_bitvec_to_table_scope(abstract_structure.used_table_bit_vector)
                result.append(__wrapper(node_in_IR, scope_set, i))
            return result
        if isinstance(abstract_structure, AbstractJoinStructure):
            scope_set = convert_table_bitvec_to_table_scope(abstract_structure.used_table_bit_vector)
            scope_set_for_quals = convert_table_bitvec_to_table_scope(abstract_structure.quals_scope_bit_vec)
            nodes_in_IR: List[Node] = []
            rename_renaming_list_hole = Hole_Rename_Renaming_List(0, scope_set)
            larg: List[Node] = __extend_helper(abstract_structure.larg, _max_wrap_level, _min_wrap_level)
            rarg: List[Node] = __extend_helper(abstract_structure.rarg, _max_wrap_level, _min_wrap_level)
            for l in larg:
                for r in rarg:
                    nodes_in_IR.append(Rename(rename_renaming_list_hole,
                        JoinExpr(abstract_structure.jointype, larg=l, rarg=r, quals=Hole_Join_On_Predicate(0, scope_set_for_quals))))
            nodes_after_wrapping: List[Node] = []
            for i in range(_min_wrap_level, _max_wrap_level+1):
                for n in nodes_in_IR:
                    nodes_after_wrapping.append(__wrapper(n, scope_set, i))
            return nodes_after_wrapping

    filling_results: List[Node] = __extend_helper(abstract_structure, max_wrap_level, min_wrap_level)
    return filling_results

def generate_sketches(schema:TableSchemaEnv, join_structures:List[JoinStructureInfo], required_table_possibilities=None, max_wrap_level=1)->List[Sketch]:
    tables = schema.get_table_names()
    if required_table_possibilities is None:
        required_table_possibilities = []
    results = generate_abstract_sketches(tables, join_structures, required_table_possibilities)
    # for r in results:
    #     print(r)
    results_in_IR: List[Node] = []
    for r in results:
        results_in_IR += convert_abstract_structure_to_IR(r, tables, max_wrap_level=max_wrap_level)
    return [Sketch(i) for i in results_in_IR]


def generate_annotated_sketch(sketch, filters):
    filter_allocations = []
    sketches = []

    for i in range(len(filters)):
        allocations_for_each_filter = [tuple([-1, -1])]

        for j in range(len(filters[i])):
            this_filter = filters[i][j]

            for hole in sketch.holes:
                if len(hole.scope) != 0 and this_filter.scope.issubset(hole.scope):
                    if isinstance(this_filter, Group_Filter) and isinstance(hole, Hole_Column_List):
                        allocations_for_each_filter.append(tuple([j, hole.id]))
                    elif this_filter.scope == hole.scope:
                        if isinstance(this_filter, Predicate_Filter):
                            if isinstance(hole, Hole_Where_Predicate):
                                allocations_for_each_filter.append(tuple([j, hole.id]))
                            elif isinstance(hole, Hole_Having_Predicate) and this_filter.has_agg:
                                allocations_for_each_filter.append(tuple([j, hole.id]))
                        elif isinstance(this_filter, Join_Condition_Filter) and isinstance(hole,
                                                                                           Hole_Join_On_Predicate):
                            allocations_for_each_filter.append(tuple([j, hole.id]))

        filter_allocations.append(allocations_for_each_filter)

    for info in list(itertools.product(*filter_allocations)):
        filter_sz = len(info)
        # print(info)
        annotated_sketch = deepcopy(sketch)
        try:
            for k in range(filter_sz):
                annotated_sketch.filter_indicator.append(info[k][0])
                annotated_sketch.filter_location.append(info[k][1])

                # if it is a real hole
                if info[k][1] != -1:
                    if info[k][1] not in annotated_sketch.hole_filters_dict.keys():
                        annotated_sketch.hole_filters_dict[info[k][1]] = [filters[k][info[k][0]]]
                    else:
                        # raise Exception("we don't allow multiple assignments")
                        annotated_sketch.hole_filters_dict[info[k][1]].append(filters[k][info[k][0]])
        except:
            continue
        # print(annotated_sketch.hole_filters_dict)
        sketches.append(annotated_sketch)

    return sketches
