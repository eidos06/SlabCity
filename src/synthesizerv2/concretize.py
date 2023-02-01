from typing import Tuple, List
from synthesizerv2.chain import *
from synthesizerv2.sketch import *
from synthesizerv2.filter import *
from synthesizerv2.chain import _chain_extend_helper_output_node
from synthesizerv2.hole import *
import itertools
from synthesizerv2.analysis import ChainInfo, FreshColToBaseTableMapping, AggFuncGetter
from synthesizerv2.basics import FreshColCounterEnv, TableSchemaEnv, BaseColumn, deepcopy
from experiments.exp_basics import get_alpha_equiv_hash
from pglast.ast import *
from pglast.enums import *



def _get_required_chain_helper(annotated_sketch: Sketch,
                               hole) -> ChainInfo:
    result_chain = []
    analyzed_filters = annotated_sketch.get_filters_by_id(hole.id)
    if len(analyzed_filters) == 0:
        return ChainInfo(result_chain)
    for f in analyzed_filters:
        if isinstance(f, Predicate_Filter):
            if isinstance(f.content, A_Expr):
                if isinstance(f.content.lexpr, Chain):
                    result_chain.append(f.content.lexpr)
                if isinstance(f.content.rexpr, Chain):
                    result_chain.append(f.content.rexpr)
            else:
                raise Exception("We currently only support A_Expr for Predicate Filter")
        if isinstance(f, Group_Filter):
            for c in f.content:
                if isinstance(c, Chain):
                    result_chain.append(c)
        if isinstance(f, Join_Condition_Filter):
            if isinstance(f.content, A_Expr):
                if isinstance(f.content.lexpr, Chain):
                    result_chain.append(f.content.lexpr)
                if isinstance(f.content.rexpr, Chain):
                    result_chain.append(f.content.rexpr)
            else:
                raise Exception("We currently only support A_Expr for Predicate Filter")

    return ChainInfo(result_chain)


def _get_assigned_col_from_chains(required: Chain, has: ChainInfo) -> Node:
    for c in has.chains:
        if required.to_compact_form() == c.to_compact_form():
            if isinstance(c.chain[-1], FreshColChainItem):
                return c.chain[-1].pointer
            raise Exception("the last item of chain is not FreshCol?")
        if is_chain_prefix(c.to_compact_form(), required.to_compact_form()):
            chain_cut_result = cut_chain(required.to_compact_form(), c.to_compact_form())
            if len(chain_cut_result) == 1:
                result = _chain_extend_helper_output_node(c, chain_cut_result)
                return result[0][0]
    return None


def _concretize_hole_helper(annotated_sketch, hole, current_chain: ChainInfo) -> Node:
    analyzed_filters = annotated_sketch.get_filters_by_id(hole.id)
    if len(analyzed_filters) == 0:
        return None
    expr_list = []
    for f in analyzed_filters:
        if isinstance(f, Predicate_Filter):
            if isinstance(f.content, A_Expr):
                concretized_expr = deepcopy(f.content)
                if isinstance(concretized_expr.lexpr, Chain):
                    concretized_expr.lexpr = _get_assigned_col_from_chains(concretized_expr.lexpr, current_chain)
                if isinstance(concretized_expr.rexpr, Chain):
                    concretized_expr.rexpr = _get_assigned_col_from_chains(concretized_expr.rexpr, current_chain)
                # sanity check: if it's in a where hole, we forbid generating agg functions
                if isinstance(hole, Hole_Where_Predicate) and (isinstance(concretized_expr.lexpr,FuncCall) or isinstance(concretized_expr.rexpr, FuncCall)):
                    return None
                if concretized_expr.lexpr is None or concretized_expr.rexpr is None:
                    return None
                expr_list.append(concretized_expr)
            # raise Exception("We currently only support A_Expr as predicate filter")
        if isinstance(f, Join_Condition_Filter):
            if isinstance(f.content, A_Expr):
                concretized_expr = deepcopy(f.content)
                if isinstance(concretized_expr.lexpr, Chain):
                    concretized_expr.lexpr = _get_assigned_col_from_chains(concretized_expr.lexpr, current_chain)
                if isinstance(concretized_expr.rexpr, Chain):
                    concretized_expr.rexpr = _get_assigned_col_from_chains(concretized_expr.rexpr, current_chain.reverse())
                if concretized_expr.lexpr is None or concretized_expr.rexpr is None:
                    return None
                if isinstance(hole, Hole_Join_On_Predicate) and (
                        isinstance(concretized_expr.lexpr, FuncCall) or isinstance(concretized_expr.rexpr, FuncCall)):
                    return None
                expr_list.append(concretized_expr)
        if isinstance(f, Group_Filter):
            new_group_by_cols = []
            for g in f.content:
                if isinstance(g, Chain):
                    new_group_by_cols.append(_get_assigned_col_from_chains(g, current_chain))
            if new_group_by_cols == []:
                return None
            return new_group_by_cols

    if len(expr_list) == 1:
        return expr_list[0]
    else:
        new_bool_expr = BoolExpr()
        new_bool_expr.boolop = BoolExprType.AND_EXPR
        new_bool_expr.args = tuple(expr_list)
        return new_bool_expr

    # raise Exception("We currently don't support queries that has more than one filter allocation.")


class ConcretizeInfo:
    def __init__(self, node: Node,
                 chainInfo: ChainInfo,
                 mapping: FreshColToBaseTableMapping,
                 exposed_columns: List[Node]):
        self.node = node
        self.chainInfo = chainInfo
        self.mapping = mapping
        self.exposed_columns = exposed_columns


"""
concretize annotated_sketch from innermost to outer most.

input: annotated_sketch -> sketch with filter allocation
       schema -> schema info for the sketch
       node -> node to be concretized
       required_targetListInfo -> chain required for this stage
       fresh_col_counter_env -> used to generate fresh col
       
output: node:Node -> currently concretized node
        chainInfo:ChainInfo -> current has chains
        mapping:FreshColToBaseTableMapping -> mapping
        List[FreshCol] -> current exposed freshcols
"""


def _concretize_helper(annotated_sketch,
                       schema: TableSchemaEnv,
                       node,
                       required_targetListInfo: ChainInfo,
                       fresh_col_counter_env: FreshColCounterEnv) -> List[ConcretizeInfo]:
    if isinstance(node, RangeSubselect):
        return _concretize_helper(annotated_sketch, schema, node.subquery, required_targetListInfo,
                                  fresh_col_counter_env)

    if isinstance(node, RangeVar):
        chains = []
        exposed_list = []
        for base_column in schema.find_schema(node.relname).get_base_columns():
            chains.append(Chain([BaseColumnChainItem(base_column.table, base_column.colname)]))
            exposed_list.append(base_column)
        return [
            ConcretizeInfo(RangeVar(relname=node.relname, inh=True), ChainInfo(chains), FreshColToBaseTableMapping({}),
                           exposed_list)]

    if isinstance(node, JoinExpr):
        result = []
        # get required chain of this stage(quals)
        if node.quals is not None:
            required_chain: ChainInfo = _get_required_chain_helper(annotated_sketch, node.quals)
            newly_required = required_targetListInfo.union(required_chain)
        else:
            newly_required = required_targetListInfo
        # concretize inner expression
        results_left: List[ConcretizeInfo] = _concretize_helper(annotated_sketch, schema, node.larg,
                                                                newly_required, fresh_col_counter_env)
        results_right: List[ConcretizeInfo] = _concretize_helper(annotated_sketch, schema, node.rarg,
                                                                 newly_required, fresh_col_counter_env)
        # concretize filters
        for c_left in results_left:
            for c_right in results_right:
                # if there's qual, then concretize it
                if node.quals is not None:
                    new_quals = _concretize_hole_helper(annotated_sketch, node.quals,
                                                        c_right.chainInfo.union(c_left.chainInfo))

                    # validaty check 3
                    # currently, we do not support cross join in the extraction as well as generation
                    if new_quals is None:
                        continue
                    result.append(ConcretizeInfo(
                        JoinExpr(larg=c_left.node, rarg=c_right.node, quals=new_quals, jointype=node.jointype,
                                 isNatural=node.isNatural),
                        c_left.chainInfo.union(c_right.chainInfo),
                        c_left.mapping.union(c_right.mapping),
                        c_left.exposed_columns + c_right.exposed_columns
                    ))
        return result

    if isinstance(node, Rename):
        query_concretized_results = _concretize_helper(annotated_sketch, schema, node.query,
                                                       required_targetListInfo, fresh_col_counter_env)
        result = []
        for r in query_concretized_results:
            rename_renaming_list = []
            new_mapping = deepcopy(r.mapping)
            new_chain_info = deepcopy(r.chainInfo)
            for i in range(len(r.exposed_columns)):
                fresh_col = fresh_col_counter_env.get_fresh_col()
                rename_renaming_list.append(fresh_col)
                new_mapping.mapping[fresh_col] = r.exposed_columns[i]
                new_chain_info.chains[i].add(FreshColChainItem(fresh_col.col_index, fresh_col))
            result.append(ConcretizeInfo(Rename(tuple(rename_renaming_list), r.node),
                                         new_chain_info,
                                         new_mapping,
                                         rename_renaming_list))
        return result

    if isinstance(node, SelectStmt):
        result = []
        result_hash_list = set()

        required_chain = required_targetListInfo
        if node.whereClause is not None:
            required_chain = required_chain.union(
                _get_required_chain_helper(annotated_sketch, node.whereClause))
        if node.havingClause is not None:
            required_chain = required_chain.union(
                _get_required_chain_helper(annotated_sketch, node.havingClause))
        if node.groupClause is not None:
            required_chain = required_chain.union(
                _get_required_chain_helper(annotated_sketch, node.groupClause))

        query_concretized_results = _concretize_helper(annotated_sketch,
                                                       schema,
                                                       node.fromClause[0],
                                                       required_chain,
                                                       fresh_col_counter_env)
        for r in query_concretized_results:
            new_where_clause = None
            new_having_clause = None
            new_group_clause = None

            # concretize filters
            if node.whereClause is not None:
                new_where_clause = _concretize_hole_helper(annotated_sketch, node.whereClause, r.chainInfo)
            if node.havingClause is not None:
                new_having_clause = _concretize_hole_helper(annotated_sketch, node.havingClause, r.chainInfo)
            if node.groupClause is not None:
                new_group_clause = _concretize_hole_helper(annotated_sketch, node.groupClause, r.chainInfo)
                if new_group_clause is not None:
                    new_group_clause = [i for i in new_group_clause if i is not None]

            # validaty check 1
            # if there is only a having clause but no group by clause, then this statement is invalid
            if new_group_clause is None and new_having_clause is not None:
                continue

            # validaty check 2
            # if there is a function in group by clause, then this statement is invalid
            if new_group_clause is not None:
                agg_func_set = set()
                for idx, val in enumerate(new_group_clause):
                    agg_func_set = agg_func_set.union(AggFuncGetter()(val))
                if len(agg_func_set) > 0:
                    continue


            # determine target list
            # possible_target_list = []
            # for require in required_targetListInfo.chains:
            #     require_chain_compact_form = require.to_compact_form()
            #     had_chain_with_shortest_distance = None
            #     current_length = 0
            #     for had_chain in r.chainInfo.chains:
            #         had_chain_compact_form = had_chain.to_compact_form()
            #         new_length = len(had_chain_compact_form)
            #         if is_chain_prefix(had_chain_compact_form, require_chain_compact_form):
            #             if had_chain_with_shortest_distance is None or new_length > current_length:
            #                 had_chain_with_shortest_distance = had_chain
            #                 current_length = new_length
            #     if had_chain_with_shortest_distance is not None:
            #         possible_target_list.append(get_possible_extension_for_chain(had_chain_with_shortest_distance, require))

            possible_target_list = []
            for require in required_targetListInfo.chains:
                result_for_each_target = []
                require_chain_compact_form = require.to_compact_form()
                for had_chain in r.chainInfo.chains:
                    had_chain_compact_form = had_chain.to_compact_form()
                    if is_chain_prefix(had_chain_compact_form, require_chain_compact_form):
                        result_for_each_target.extend(get_possible_extension_for_chain(had_chain, require))
                if len(result_for_each_target) != 0:
                    possible_target_list.append(result_for_each_target)

            all_possible_combination = itertools.product(*possible_target_list)
            # construct results
            for t in all_possible_combination:
                nodes = []
                columns_list = []
                chains = []
                for extend_result in t:
                    if extend_result[0] in columns_list:
                        continue
                    columns_list.append(extend_result[0])
                    nodes.append(ResTarget(val=extend_result[0]))
                    chains.append(extend_result[1])
                
                # validaty check 4
                # we cannot select columns that do not appear in group by clauses
                nodes_after_validation = []
                chains_after_validation = []
                agg_func_set = set()
                # append functions to nodes after validation
                for idx, tmp in enumerate(nodes):
                    func_set = AggFuncGetter()(tmp)
                    if len(func_set) != 0:
                        nodes_after_validation.append(nodes[idx])
                        chains_after_validation.append(chains[idx])
                    agg_func_set = agg_func_set.union(func_set)
                # append columns to nodes after validation
                if (new_group_clause is None and len(agg_func_set) > 0) or new_group_clause is not None:
                    if new_group_clause is None:
                        group_col_idx_list = []
                    else:
                        group_col_idx_list = [i.col_index for i in new_group_clause]
                    for idx, tar in enumerate(nodes):
                        if isinstance(tar.val, FreshCol) and tar.val.col_index in group_col_idx_list:
                            nodes_after_validation.append(nodes[idx])
                            chains_after_validation.append(chains[idx])
                else:
                    nodes_after_validation = nodes
                    chains_after_validation = chains
                nodes = nodes_after_validation
                chains = chains_after_validation
                # --------- end of check 4 -------------

                # result.append(ConcretizeInfo(SelectStmt(
                #     targetList=nodes,
                #     whereClause=new_where_clause,
                #     havingClause=new_having_clause,
                #     groupClause=new_group_clause,
                #     fromClause=tuple([r.node])
                # ), ChainInfo(chains), r.mapping, nodes))

                if len(nodes) == 0:
                    continue

                tmp_node = SelectStmt(
                    targetList=nodes,
                    whereClause=new_where_clause,
                    havingClause=new_having_clause,
                    groupClause=new_group_clause,
                    fromClause=tuple([r.node])
                )
                query_hash = get_alpha_equiv_hash(tmp_node)
                if query_hash in result_hash_list:
                    continue
                else:
                    result_hash_list.add(query_hash)
                    result.append(ConcretizeInfo(tmp_node, ChainInfo(chains), r.mapping, nodes))
        return result


def concretize(annotated_sketch: Sketch, schema, targetListInfo: ChainInfo) -> List[Node]:
    output = []
    results = _concretize_helper(annotated_sketch, schema, annotated_sketch.query, targetListInfo, FreshColCounterEnv())
    for r in results:
        after_cut_list = cut_chains(targetListInfo.chains, r.chainInfo.chains)
        flag = False
        for i in after_cut_list:
            if len(i) != 0:
                flag = True
                break
        if not flag:
            #yield r.node
            output.append(r.node)
    return output
