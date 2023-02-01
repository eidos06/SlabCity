import itertools
import more_itertools
from synthesizerv2.basics import *
from synthesizerv2.filter import *
from pglast.ast import *
from pglast.visitors import Visitor
from pglast.enums.nodes import JoinType
from typing import List
from synthesizerv2.chain import *


class FreshColToBaseTableMapping:
    def __init__(self, mapping):
        self.mapping = mapping

    def get_base_column(self, freshcol) -> BaseColumn:
        while freshcol in self.mapping:
            freshcol = self.mapping[freshcol]
        if isinstance(freshcol, BaseColumn):
            return freshcol
        if isinstance(freshcol, FuncCall):
            return self.get_base_column(freshcol.args[0])
        raise Exception("Can't get base column of " + str(freshcol))

    def union(self, other):
        tmp = deepcopy(self.mapping)
        tmp.update(other.mapping)
        return FreshColToBaseTableMapping(tmp)

    def __deepcopy__(self, memodict={}):
        new_mapping = {}
        for key, value in self.mapping.items():
            new_mapping[key] = deepcopy(value)
        return FreshColToBaseTableMapping(new_mapping)


class JoinStructureInfo:
    def __init__(self, ltable, rtable, join_type=None):
        self.ltable = ltable
        self.rtable = rtable
        self.join_type = join_type

    def __eq__(self, other):
        return self.ltable == other.ltable and self.rtable == other.rtable

    def __hash__(self) -> int:
        return hash(self.ltable + "&" + self.rtable)



class FreshColGetter(Visitor):
    def __init__(self):
        self.fresh_cols = set()

    def __call__(self, node):
        super().__call__(node)
        return self.fresh_cols

    def visit(self, ancestors, node):
        if isinstance(node, FreshCol):
            self.fresh_cols.add(node)


class AggFuncGetter(Visitor):
    def __init__(self):
        self.functions = set()

    def __call__(self, node):
        super().__call__(node)
        return self.functions

    def visit(self, ancestors, node):
        if isinstance(node, FuncCall):
            agg_func_names = {"max", "min", "count", "sum", "avg"}
            if node.funcname[0].val in agg_func_names:
                self.functions.add(node)

class TableGetter(Visitor):
    def __init__(self):
        self.tables = set()

    def __call__(self, node):
        super().__call__(node)
        return self.tables

    def visit(self, ancestors, node):
        if isinstance(node, RangeVar):
            self.tables.add(node.relname)


class ChainInfo:
    def __init__(self, chains=[]):
        self.chains: List[Chain] = chains

    def union(self, other):
        return ChainInfo(self.chains + other.chains)

    def reverse(self):
        tmp = deepcopy(self.chains)
        tmp.reverse()
        return ChainInfo(tmp)


def analyze_base_table_and_columns(input_query: QueryWithSchema):
    def helper(node: Node, schema: TableSchemaEnv):
        mapping = {}
        if isinstance(node, Rename):
            if isinstance(node.query, SelectStmt):
                for i in range(0, len(node.list_variables)):
                    mapping[node.list_variables[i]] = node.query.targetList[i].val
                mapping.update(helper(node.query.fromClause[0], schema))
                return mapping
            if isinstance(node.query, RangeSubselect):
                if isinstance(node.query.subquery, SelectStmt):
                    for i in range(0, len(node.list_variables)):
                        mapping[node.list_variables[i]] = node.query.subquery.targetList[i].val
                mapping.update(helper(node.query.subquery.fromClause[0], schema))
                return mapping
            if isinstance(node.query, JoinExpr):
                left_mapping = {}
                right_mapping = {}
                if isinstance(node.query.larg, Rename):
                    for i in range(0, len(node.query.larg.list_variables)):
                        left_mapping[node.list_variables[i]] = node.query.larg.list_variables[i]
                if isinstance(node.query.rarg, Rename):
                    for i in range(len(node.query.larg.list_variables),
                                   len(node.query.larg.list_variables) + len(node.query.rarg.list_variables)):
                        right_mapping[node.list_variables[i]] = node.query.rarg.list_variables[
                            i - len(node.query.larg.list_variables)]
                mapping.update(left_mapping)
                mapping.update(right_mapping)
                mapping.update(helper(node.query.larg, schema))
                mapping.update(helper(node.query.rarg, schema))
                return mapping
            if isinstance(node.query, RangeVar):
                base_columns = schema.find_schema(node.query.relname).get_base_columns()
                for i in range(len(node.list_variables)):
                    mapping[node.list_variables[i]] = base_columns[i]
                return mapping

    result_mapping = helper(input_query.query, input_query.schema)
    return FreshColToBaseTableMapping(result_mapping)


def analyze_table_structure(query: QueryWithSchema,
                            mapping: FreshColToBaseTableMapping):
    def helper(node: Node, mapping: FreshColToBaseTableMapping):
        join_structures: List[JoinStructureInfo] = []
        if isinstance(node, Rename):
            if isinstance(node.query, SelectStmt):
                join_structures = join_structures + helper(node.query.fromClause[0], mapping)
                return join_structures
            if isinstance(node.query, RangeSubselect):
                join_structures = join_structures + helper(node.query.subquery.fromClause[0], mapping)
                return join_structures
            if isinstance(node.query, JoinExpr):
                join_type = node.query.jointype
                database_tables = query.schema.schemas.keys()
                exposed_left_tables = TableGetter()(node.query.larg) & database_tables
                exposed_right_tables = TableGetter()(node.query.rarg) & database_tables

                if isinstance(node.query.quals, A_Expr):
                    expr = node.query.quals
                    table1 = mapping.get_base_column(expr.lexpr).table
                    table2 = mapping.get_base_column(expr.rexpr).table

                    if table1 in exposed_left_tables and table2 in exposed_right_tables:
                        join_structures.append(JoinStructureInfo(table1, table2, join_type))
                    else:
                        join_structures.append(JoinStructureInfo(table2, table1, join_type))
                elif node.query.quals is not None:
                    table_comb_set = set()
                    for ele in node.query.quals.args:
                        table1 = mapping.get_base_column(ele.lexpr).table
                        table2 = mapping.get_base_column(ele.rexpr).table
                        table_comb_set.add(frozenset({table1, table2}))

                    for comb in table_comb_set:
                        comb = sorted(list(comb))
                        if len(comb) == 1:
                            join_structures.append(JoinStructureInfo(comb[0], comb[0], join_type))
                        elif len(comb) == 2:
                            if comb[0] in exposed_left_tables and comb[1] in exposed_right_tables:
                                join_structures.append(JoinStructureInfo(comb[0], comb[1], join_type))
                            else:
                                join_structures.append(JoinStructureInfo(comb[1], comb[0], join_type))

                join_structure_inside_both_side = helper(node.query.larg, mapping) + helper(node.query.rarg, mapping)
                join_structures += join_structure_inside_both_side
                return join_structures
            if isinstance(node.query, RangeVar):
                return []

    # for now, we only consider single chain in a target
    # therefore, the first item in that chain is a single base column

    result_join_structure = helper(query.query, mapping)
    return result_join_structure


def extract_filters(query: QueryWithSchema, info_conditions, mapping: FreshColToBaseTableMapping):
    class FilterGetter(Visitor):
        def __init__(self):
            self.filters = []

        def __call__(self, node):
            super().__call__(node)
            return self.filters

        def visit(self, ancestors, node):
            if isinstance(node, SelectStmt):
                if node.groupClause != None:
                    group_filter_set = set()

                    original_scope_set = set()
                    original_col_list = []
                    col_eq_list = []
                    for idx, group_col in enumerate(node.groupClause):
                        original_scope_set.add(mapping.get_base_column(group_col).table)
                        tmp_chain = _chain_extend_helper(Chain(), group_col, mapping).to_compact_form()
                        original_col_list.append(tmp_chain)

                        # check all the equations
                        for s in info_conditions:
                            if tmp_chain.chain[0] in s:
                                remaining_set = s - {tmp_chain.chain[0]}
                                for col in remaining_set :
                                    new_chain = Chain()
                                    new_chain.add(col)
                                    col_eq_list.append(new_chain)
                    group_filter_set.add(Group_Filter(original_col_list, original_scope_set))

                    for col in col_eq_list:
                        group_filter_set.add(Group_Filter([col], {new_chain.chain[0].table}))

                    # try to do group by push down
                    alternative_scope_set = set()

                    agg_func_set = set()
                    if node.havingClause is not None:
                        agg_func_set = agg_func_set.union(AggFuncGetter()(node.havingClause))
                    for ele in node.targetList:
                        agg_func_set = agg_func_set.union(AggFuncGetter()(ele))

                    for func in agg_func_set:
                        cols = FreshColGetter()(func)
                        for col in cols:
                            alternative_scope_set.add(mapping.get_base_column(col).table)

                    tb_cols_dict = dict()
                    for tb in alternative_scope_set:
                        tb_cols_dict[tb] = set()

                    for agg_tb in alternative_scope_set:
                        for s in info_conditions:
                            for ele in s:
                                if ele.table == agg_tb:
                                    tb_cols_dict[agg_tb].add(ele)
                        alternative_cols = list(more_itertools.powerset(tb_cols_dict[agg_tb]))
                        alternative_cols = [list(x) for x in alternative_cols if len(x) > 0]
                        for col_list in alternative_cols:
                            chain_list = []
                            for col in col_list:
                                chain_list.append(Chain().add(col))
                            group_filter_set.add(Group_Filter(chain_list, {agg_tb}))

                    self.filters.append(list(group_filter_set))

            elif isinstance(node, A_Expr):
                scope_set = set()
                fresh_cols = FreshColGetter()(node)
                for col in fresh_cols:
                    scope_set.add(mapping.get_base_column(col).table)
                if not isinstance(node.lexpr, A_Const):
                    node.lexpr = _chain_extend_helper(Chain(), node.lexpr, mapping).to_compact_form()
                if not isinstance(node.rexpr, A_Const):
                    node.rexpr = _chain_extend_helper(Chain(), node.rexpr, mapping).to_compact_form()

                if isinstance(node.lexpr, Chain) and isinstance(node.rexpr, Chain):
                    if len(scope_set) > 1:
                        self.filters.append([Join_Condition_Filter(node, scope_set)])
                    else:
                        if (isinstance(node.lexpr, Chain) and node.lexpr.include_functions()) or (
                            isinstance(node.rexpr, Chain) and node.rexpr.include_functions()):
                            self.filters.append([Predicate_Filter(node, scope_set, True), Join_Condition_Filter(node, scope_set)])
                        else:
                            self.filters.append([Predicate_Filter(node, scope_set, False), Join_Condition_Filter(node, scope_set)])
                else:
                    if (isinstance(node.lexpr, Chain) and node.lexpr.include_functions()) or (
                            isinstance(node.rexpr, Chain) and node.rexpr.include_functions()):
                        self.filters.append([Predicate_Filter(node, scope_set, True)])
                    else:
                        self.filters.append([Predicate_Filter(node, scope_set, False)])

    filters = FilterGetter()(query.query)
    return filters


def get_mapping_item(mapping: FreshColToBaseTableMapping, item):
    if item in mapping.mapping:
        return mapping.mapping[item]
    else:
        return None


def _chain_extend_helper(chain, currentItem, mapping):
    if isinstance(currentItem, FreshCol):
        chain = _chain_extend_helper(chain, get_mapping_item(mapping, currentItem), mapping)
        return chain.add(FreshColChainItem(currentItem.col_index))
    if isinstance(currentItem, FuncCall):
        if len(currentItem.args) != 1:
            raise Exception("currently we only support functions with 1 argument")
        chain = _chain_extend_helper(chain, get_mapping_item(mapping, currentItem.args[0]), mapping)
        return chain.add(FunctionChainItem(currentItem.funcname[0].val))
    if isinstance(currentItem, BaseColumn):
        return chain.add(BaseColumnChainItem(currentItem.table, currentItem.colname))
    raise Exception("currently we only support functions and FreshCol and BaseCol on the chain.")


def analyze_target_list_chains(translated_query, mapping):
    if not isinstance(translated_query, Rename):
        raise Exception("the translated query must in the form of a Rename.")
    chain_starts = translated_query.list_variables
    chains: List[Chain] = []
    for chain_head in chain_starts:
        chains.append(_chain_extend_helper(Chain(), chain_head, mapping).to_compact_form())
    return ChainInfo(chains)


def analyze_join_conditions(translated_query, mapping):
    class JoinConditionGetter(Visitor):
        def __init__(self):
            self.conditions = []
        def __call__(self, node):
            super().__call__(node)
            return self.conditions
        def visit(self, ancestors, node):
            if isinstance(node, A_Expr):
                if (not isinstance(node.lexpr, A_Const)) and (not isinstance(node.rexpr, A_Const)):
                    lhs = _chain_extend_helper(Chain(), node.lexpr, mapping).to_compact_form()
                    rhs = _chain_extend_helper(Chain(), node.rexpr, mapping).to_compact_form()
                    left_content = lhs.chain[0]
                    right_content = rhs.chain[0]

                    already_exist = False
                    for s in self.conditions:
                        if left_content in s or right_content in s:
                            s.add(left_content)
                            s.add(right_content)
                            already_exist = True

                    if not already_exist:
                        self.conditions.append({left_content, right_content})

    conditions = JoinConditionGetter()(translated_query)
    return conditions

def analyze_necessary_table_sets(info_targetlist: ChainInfo, conditions):
    try_all_combinations = []
    for chain in info_targetlist.chains:
        col = chain.chain[0]

        # if only one table is involved, then we can confirm that this column comes from that table
        # otherwise, we need to list out all the possibilities e.g., (a, b) -> (a), (b), (a, b)
        only_one_table = True
        for s in conditions:
            table_set = set()
            for ele in s:
                table_set.add(ele.table)

            if col in s:
                combinations = list(more_itertools.powerset(table_set))
                combinations = [set(x) for x in combinations if len(x) > 0]
                try_all_combinations.append(combinations)
                only_one_table = False 

        if only_one_table:
            try_all_combinations.append([{col.table}])

    result_set = set()
    for list_of_set in itertools.product(*try_all_combinations):
        candidate_set = set()
        for s in list_of_set:
            candidate_set = candidate_set.union(s)
        result_set.add(frozenset(candidate_set))

    final_result_set = [set(element_set) for element_set in result_set]
    return final_result_set
