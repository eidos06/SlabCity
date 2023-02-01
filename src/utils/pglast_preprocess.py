from pglast.ast import *
from pglast.enums import JoinType, SortByDir, BoolExprType, SetOperation

from lib.basics import AliasDicForPreProcess
from lib.basics import Schema
from lib.dsl import *
from lib.exceptions import TranslationNotSupportedException
from utils.deepcopy import deepcopy


def preprocessing(query: Node, schema: Schema)->Node:
    UniqueIdGenerator.reset()
    query = deepcopy(query)
    return __preprocessing(query, schema)[0]

def __preprocessing(query: Node, schema: Schema, alias_mapping: AliasDicForPreProcess = AliasDicForPreProcess()) -> \
Tuple[Node, AliasDicForPreProcess]:
    """
    translate a pglast query to dsl
    Args:
        complement_level: NOT management
        query: pglast ast
        schema: schema used in the query
        alias_mapping: a dictionary that records the Alias

    Returns:
        result: SQLComponent translated
        AliasDicForPreProcess: a dictionary that records alias
    """
    if isinstance(query, SelectStmt):
        if isinstance(query.op, SetOperation):
            if query.op != SetOperation.SETOP_NONE:
                return __preprocessing(query.larg, schema, alias_mapping)
        # special case for value lists
        if query.valuesLists is not None:
            return query, AliasDicForPreProcess()
        # when translating SelectStmt, first translate its FROM clause
        if len(query.fromClause) != 1:
            raise TranslationNotSupportedException("not supported from clause with multiple arguments")
        from_clause_component, alias_dic = __preprocessing(query.fromClause[0], schema,
                                                         alias_mapping)

        # then translate having
        if query.havingClause is not None:
            having_clause, alias_dic = __preprocessing(query.havingClause, schema, alias_dic)
        else:
            having_clause = None

        # then translate target list
        select_clause_list: List[Node] = []
        new_alias_dic = AliasDicForPreProcess()
        for res_target in query.targetList:
            # translation
            item, alias_dic = __preprocessing(res_target, schema, alias_dic)
            # special case for A_Star
            if isinstance(res_target.val, ColumnRef):
                if isinstance(res_target.val.fields[0], A_Star):
                    # in case of A_Star, we add all columns available
                    available_cols = alias_dic.get_all_available_column()
                    for col in available_cols:
                        select_clause_list.append(ResTarget(val=col))
                        t, c = alias_dic.get_full_alias(col.fields)
                        new_alias_dic.record_alias(c, t)
                    continue


            # alias
            if isinstance(res_target.val, ColumnRef):
                t, c = alias_dic.get_full_alias(res_target.val.fields)
                if res_target.name is None:
                    new_alias_dic.record_alias(c, t)
                else:
                    new_alias_dic.record_alias(res_target.name, t)
            else:
                if res_target.name is None:
                    res_target.name = f"tmp{UniqueIdGeneratorFor.get_unique_id()}"
                new_alias_dic.record_alias(res_target.name, "")
            select_clause_list.append(item)

        new_select:SelectStmt = deepcopy(query)
        new_select.targetList = tuple(select_clause_list)
        new_select.havingClause = having_clause
        new_select.fromClause = tuple([from_clause_component])
        return new_select, new_alias_dic

    if isinstance(query, RangeSubselect):
        from_clause_component, alias_dic = __preprocessing(query.subquery, schema, alias_mapping)
        new_subselect:RangeSubselect = deepcopy(query)
        new_subselect.subquery = from_clause_component
        if query.alias is not None:
            if query.alias.colnames is not None:
                #only include colnames, if colnames alias are defined
                new_alias_dic = AliasDicForPreProcess()
                table_alias = query.alias.aliasname
                for colname in query.alias.colnames:
                    col_alias = colname.val
                    new_alias_dic.record_alias(col_alias, table_alias)
                return new_subselect, new_alias_dic
            else:
                # exposed space will be replaced, for col names not redefined
                new_alias_dic = AliasDicForPreProcess()
                for k, v in alias_dic.get_items():
                    new_alias_dic.record_alias(k.column, query.alias.aliasname)
                return new_subselect, new_alias_dic
        else:
            raise TranslationNotSupportedException("subselect doesn't have alias")

    if isinstance(query, RangeVar):
        table_name = query.relname
        table = schema.get_table(table_name)
        table = Table(table_name, table.get_cols())
        new_alias_dic = AliasDicForPreProcess()
        for c in table.table_cols:
            if query.alias is None:
                new_alias_dic.record_alias(c[0], table.table_name)
            else:
                new_alias_dic.record_alias(c[0], query.alias.aliasname)
        if query.alias is None:
            return query, new_alias_dic
        else:
            return query, new_alias_dic

    if isinstance(query, JoinExpr):
        # first deal with two tables
        larg_component, lalias_dic = __preprocessing(query.larg, schema, alias_mapping)
        rarg_component, ralias_dic = __preprocessing(query.rarg, schema, alias_mapping)

        new_query:JoinExpr = deepcopy(query)
        new_query.larg = larg_component
        new_query.rarg = rarg_component

        lalias_dic.combine(ralias_dic)



        if query.alias is not None:
            new_alias_dic = AliasDicForPreProcess()
            for k, v in lalias_dic.get_items():
                new_alias_dic.record_alias(k.column, query.alias.aliasname)
        else:
            new_alias_dic = lalias_dic


        return new_query, new_alias_dic

    if isinstance(query, A_Expr):
        larg, alias_dic = __preprocessing(query.lexpr, schema, alias_mapping)

        rarg, alias_dic = __preprocessing(query.rexpr, schema, alias_mapping)

        if len(query.name) != 1:
            raise TranslationNotSupportedException("operator name length > 1 in expression?")

        new_a_expr:A_Expr = deepcopy(query)
        new_a_expr.lexpr = larg
        new_a_expr.rexpr = rarg
        return new_a_expr, alias_dic

    # if isinstance(query, FuncCall):
    #     if len(query.args) != 1:
    #         raise TranslationNotSupportedException("currently we only support function with single argument")
    #     if len(query.funcname) != 1:
    #         raise TranslationNotSupportedException("currently we only support function with single name")
    #     # if there is a star inside the function, then select the first available column
    #     new_func_call:FuncCall = deepcopy(query)
    #     if query.agg_star == True:
    #         new_func_call.agg_star = False
    #         new_func_call.args = tuple([alias_mapping.get_first_available_column()])


        # return agg_component, alias_dict

    return query, alias_mapping

