from lib.dsl import *

topological_order = [
    'OpEq',
    'OpLt',
    'OpLeq',
    'OpOr',
    'OpNeq',
    'OpAnd',
    'DistinctClause',
    'Table',
    'AliasStringConst',
    'StringConst',
    'AggregationFuncNameMin',
    'AggregationFuncNameCount',
    'AggregationFuncNameMax',
    'AggregationFuncNameAvg',
    'WindowFuncNameDenseRank',
    'TableClause',
    'OrderByConfigurationASC',
    'AggregationFuncNameSum',
    'NoneClause',
    'OpAnd',
    'OrderByConfigurationDESC',
    'JoinTypeLeft',
    'FloatConst',
    'ColChain',
    'IntConst',
    'JoinTypeInner',
    'AggChain',
    'WindowFuncChain',
    'AggregationClause',
    'PartitionClauseList',
    'WindowFuncClause',
    'GroupByClauseList',
    'OrderByClauseItem',
    'OrderByClauseList',
    'PredicateBinOpLogicClause',
    'PredicateOpClause',
    'WindowFuncCallClauseAgg',
    'WindowFuncCallClauseWinFun',
    'TableClauseSource',
    'QueryComponentSource',
    'JoinWithOnClause',
    'QueryComponent',
]

terminals_to_init = [
    OpEq(),
    OpLt(),
    OpLeq(),
    OpOr(),
    OpNeq(),
    OpAnd(),
    AggregationFuncNameMin(),
    AggregationFuncNameCount(),
    AggregationFuncNameMax(),
    AggregationFuncNameAvg(),
    WindowFuncNameDenseRank(),
    AggregationFuncNameSum(),
    OrderByConfigurationASC(),
    OrderByConfigurationDESC(),
    NoneClause(),
    JoinTypeLeft(),
    JoinTypeInner(),
]

non_terminals = [
    AggChain,
    WindowFuncChain,
    AggregationClause,
    PartitionClauseList,
    WindowFuncClause,
    GroupByClauseList,
    OrderByClauseItem,
    OrderByClauseList,
    PredicateBinOpLogicClause,
    PredicateOpClause,
    WindowFuncCallClauseAgg,
    WindowFuncCallClauseWinFun,
    TableClauseSource,
    QueryComponentSource,
    JoinWithOnClause,
    QueryComponent,
]


def get_order_index(class_name: str) -> int:
    return topological_order.index(class_name)


def order_asc_full_string(a: str, b: str):
    return topological_order.index(a) < topological_order.index(b)
