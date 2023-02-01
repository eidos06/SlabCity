from synthesizerv2.hole import *
from synthesizerv2.filter import *
from synthesizerv2.sketch import *
from synthesizerv2.analysis import JoinStructureInfo
from pglast.enums import *
from typing import Set
import itertools
from synthesizerv2.basics import *
from pglast.stream import IndentedStream


class Generate_Sketch_Termination:
    pass


def _generate_sketch_helper(necessary_set: Set[JoinStructureInfo], optional_set: Set[JoinStructureInfo], table_set,
                            level, depth, hole, require_table_set: Set[str]):
    # if depth limit has been reached, then terminate
    if depth == 0:
        return [Generate_Sketch_Termination()], set()

    results = []
    scope_set = set()
    # ?Rename?
    if isinstance(hole, Hole_Rename):
        hole_rename_list = Hole_Rename_Renaming_List(level)
        hole_rename_content = Hole_Rename_Content(level)
        hole_filling_result, scope_set = _generate_sketch_helper(necessary_set, optional_set, table_set, level + 1, depth - 1,
                                                                 hole_rename_content, require_table_set)
        for filling_result in hole_filling_result:
            if isinstance(filling_result, Generate_Sketch_Termination):
                results.append(Generate_Sketch_Termination())
                continue
            results.append(Rename(hole_rename_list, filling_result))
        return results, scope_set

    # ?Rename_Content?
    if isinstance(hole, Hole_Rename_Content):
        # SELECT ?? FROM ?Rename？ WHERE ??, SELECT ?? FROM ?Rename? HAVING ?? GROUP BY ??
        hole_rename = Hole_Rename(level)
        hole_rename_filling_result, scope_set = _generate_sketch_helper(necessary_set, optional_set, table_set, level, depth, hole_rename,
                                                                        require_table_set)
        for filling_result in hole_rename_filling_result:
            if isinstance(filling_result, Generate_Sketch_Termination):
                continue
            results.append(
                RangeSubselect(subquery=SelectStmt(targetList=Hole_Target_List_With_Agg(level, scope_set), fromClause=
                filling_result, whereClause=Hole_Where_Predicate(level, scope_set),  havingClause=Hole_Having_Predicate(level, scope_set),
                                                   groupClause=Hole_Column_List(level, scope_set))))
            # results.append(
            #     RangeSubselect(subquery=SelectStmt(targetList=Hole_Target_List_With_Agg(level, scope_set), fromClause=
            #     filling_result, havingClause=Hole_Having_Predicate(level, scope_set),
            #                                        groupClause=Hole_Column_List(level, scope_set))))
            if not isinstance(filling_result, Rename):
                results.append(filling_result)

        total_set = necessary_set | optional_set
        if len(total_set) > 0:
            for i in range(len(total_set)):
                structure_copy = deepcopy(total_set)
                j = list(structure_copy)[i]

                flag = True

                if len(require_table_set) == 0:
                    rename_require_table_set_1 = {j.ltable}
                    rename_require_table_set_2 = {j.rtable}
                else:
                    if j.ltable == list(require_table_set)[0]:
                        rename_require_table_set_1 = require_table_set
                        rename_require_table_set_2 = {j.rtable}
                    elif j.rtable == list(require_table_set)[0]:
                        rename_require_table_set_1 = {j.ltable}
                        rename_require_table_set_2 = require_table_set
                    else:
                        flag = False

                structure_copy.discard(j)

                if flag:
                    new_necessary_set = structure_copy.intersection(necessary_set)
                    new_optional_set = structure_copy.intersection(optional_set)

                    rename_hole_1 = Hole_Rename(level)
                    rename_hole_2 = Hole_Rename(level)
                    rename_hole_3 = Hole_Rename(level)

                    hole_left_filling_results, left_scope_set = _generate_sketch_helper(new_necessary_set, new_optional_set, table_set, level, depth,
                                                                                        rename_hole_1,
                                                                                        rename_require_table_set_1)
                    hole_right_filling_results, right_scope_set = _generate_sketch_helper(new_necessary_set, new_optional_set, table_set, level, depth,
                                                                                          rename_hole_2,
                                                                                          rename_require_table_set_2)

                    scope_set = left_scope_set | right_scope_set
                    for result_left in hole_left_filling_results:
                        for result_right in hole_right_filling_results:
                            if isinstance(result_left, Generate_Sketch_Termination) or isinstance(result_right,
                                                                                                  Generate_Sketch_Termination):
                                continue
                            # here we consider three types of join: inner, left, cross
                            #inner
                            results.append(JoinExpr(jointype=JoinType.JOIN_INNER,
                                                    larg=result_left, rarg=result_right,
                                                    quals=Hole_Join_On_Predicate(level,
                                                                                 rename_require_table_set_1 | rename_require_table_set_2)))
                            #left * 2
                            results.append(JoinExpr(jointype=JoinType.JOIN_LEFT,
                                                    larg=result_left, rarg=result_right,
                                                    quals=Hole_Join_On_Predicate(level,
                                                                                 rename_require_table_set_1 | rename_require_table_set_2)))
                            results.append(JoinExpr(jointype=JoinType.JOIN_LEFT,
                                                    larg=result_right, rarg=result_left,
                                                    quals=Hole_Join_On_Predicate(level,
                                                                                 rename_require_table_set_1 | rename_require_table_set_2)))
                            #cross
                            results.append(JoinExpr(jointype=JoinType.JOIN_INNER,
                                                    larg=result_left, rarg=result_right))

                    if j in optional_set:
                        hole_rename_filling_result, scope_set = _generate_sketch_helper(new_necessary_set, new_optional_set, table_set, level, depth,
                                                                                    rename_hole_3,
                                                                                    require_table_set)
                        for filling_result in hole_rename_filling_result:
                            if isinstance(filling_result, Generate_Sketch_Termination):
                                continue
                            if isinstance(filling_result, Rename):
                                results.append(filling_result.query)
        else:
            if len(table_set) == 1:
                results.append(RangeVar(relname=list(table_set)[0], inh=True))
                scope_set = table_set
            else:
                if len(require_table_set) > 0:
                    results.append(RangeVar(relname=list(require_table_set)[0], inh=True))
                    scope_set = require_table_set

        if len(results) == 0:
            return [Generate_Sketch_Termination()], scope_set
        return results, scope_set


def generate_sketch(join_structure_info, schema, depth=3):
    output = set()
    necessary_set = set(join_structure_info[0]) # magic number is not good
    optional_set = set(join_structure_info[1])  # same as above
    table_set = set(schema.get_table_names())   # get_base_table and then union
    results, _ = _generate_sketch_helper(necessary_set, optional_set, table_set, 0, depth, Hole_Rename(0), set())
    for r in results:
        if isinstance(r, Rename):
            if isinstance(r.query, RangeSubselect):
                output.add(Sketch(r))
    return output


def generate_annotated_sketch(sketch, filters):
    filter_allocations = []
    sketches = []

    for i in range(len(filters)):
        allocations_for_each_filter = [tuple([-1, -1])]

        for j in range(len(filters[i])):
            this_filter = filters[i][j]

            for hole in sketch.holes:
                if len(hole.scope) != 0:
                    if this_filter.scope.issubset(hole.scope):
                        if isinstance(this_filter, Group_Filter) and isinstance(hole, Hole_Column_List):
                            allocations_for_each_filter.append(tuple([j, hole.id]))
                        elif isinstance(this_filter, Predicate_Filter):
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
        indicator_vector = []
        location_vector = []
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
                        raise Exception("we don't allow multiple assignments")
                        annotated_sketch.hole_filters_dict[info[k][1]].append(filters[k][info[k][0]])
        except:
            continue
        # print(annotated_sketch.hole_filters_dict)
        sketches.append(annotated_sketch)

    return sketches
