from pglast.ast import *
from pglast.enums import JoinType, SortByDir, BoolExprType

from lib.basics import AliasDic
from lib.basics import Schema
from lib.dsl import *
from lib.exceptions import TranslationNotSupportedException
from lib.types import DataType


def to_datatype(origin: str) -> DataType:
    if origin == "int" or origin == "decimal":
        return DataType.Number
    if origin == "varchar":
        return DataType.Str
    return DataType.Str


def pglast_to_dsl(query: Node, schema: Schema, alias_mapping: AliasDic = AliasDic(), complement_level: int = 0) -> \
Tuple[SQLComponent, AliasDic]:
    """
    translate a pglast query to dsl
    Args:
        complement_level: NOT management
        query: pglast ast
        schema: schema used in the query
        alias_mapping: a dictionary that records the Alias

    Returns:
        result: SQLComponent translated
        AliasDic: a dictionary that records alias
    """
    if isinstance(query, SelectStmt):
        # when translating SelectStmt, first translate its FROM clause
        if len(query.fromClause) != 1:
            raise TranslationNotSupportedException("not supported from clause with multiple arguments")
        from_clause_component, alias_dic = pglast_to_dsl(query.fromClause[0], schema,
                                                         alias_mapping, complement_level=complement_level)
        assert isinstance(from_clause_component, SourceClause)
        # translate target list in case of int index in group by
        target_list_references = []
        for r in query.targetList:
            ref, _ = pglast_to_dsl(r.val, schema, alias_dic, complement_level=complement_level)
            target_list_references.append(ref)

        # then translate where
        if query.whereClause is not None:
            where_component, alias_dic = pglast_to_dsl(query.whereClause, schema, alias_dic,
                                                       complement_level=complement_level)
        else:
            where_component = NoneClause()

        assert isinstance(where_component, PredicateClause)

        # then translate group by
        if query.groupClause is not None:
            references: List[ReferencableChain] = []
            for c in query.groupClause:
                # special case: int as index
                if isinstance(c, A_Const):
                    if len(target_list_references) <= c.val.val - 1:
                        tmp = target_list_references[c.val.val - 1]
                        assert isinstance(tmp, ReferencableChain)
                        references.append(tmp)
                    else:
                        continue
                elif isinstance(c, A_Expr):
                    continue
                else:
                    reference, alias_dic = pglast_to_dsl(c, schema, alias_dic, complement_level=complement_level)
                    assert isinstance(reference, ReferencableChain)
                    references.append(reference)

            group_by_component = GroupByClauseList(tuple(references))
        else:
            group_by_component = NoneClause()

        assert isinstance(group_by_component, GroupByClause)

        # then translate having
        if query.havingClause is not None:
            having_clause, alias_dic = pglast_to_dsl(query.havingClause, schema, alias_dic,
                                                     complement_level=complement_level)
        else:
            having_clause = NoneClause()

        assert isinstance(having_clause, PredicateClause)

        # then translate order by
        if query.sortClause is not None:
            component_list: List[OrderByClauseItem] = []
            for c in query.sortClause:
                # special case: int as index
                if isinstance(c.node, A_Const):
                    tmp = target_list_references[c.val.val - 1]
                    assert isinstance(tmp, ReferencableChain)
                    sort_chain = tmp
                else:
                    reference, alias_dic = pglast_to_dsl(c.node, schema, alias_dic, complement_level=complement_level)
                    assert isinstance(reference, ReferencableChain)
                    sort_chain = reference
                tmp_config = c.sortby_dir
                match tmp_config:
                    case SortByDir.SORTBY_DEFAULT | SortByDir.SORTBY_ASC:
                        sort_configuration = OrderByConfigurationASC()
                    case SortByDir.SORTBY_DESC:
                        sort_configuration = OrderByConfigurationDESC()
                    case _:
                        raise TranslationNotSupportedException("order by configuration not recognized!")
                component_list.append(OrderByClauseItem(sort_chain, sort_configuration))
            order_by_component = OrderByClauseList(tuple(component_list))
        else:
            order_by_component = NoneClause()

        # then translate target list
        select_clause_list: List[SelectClauseItem] = []
        new_alias_dic = AliasDic()
        for res_target in query.targetList:
            # translation
            item, alias_dic = pglast_to_dsl(res_target.val, schema, alias_dic, complement_level=complement_level)
            assert isinstance(item, SelectClauseItem)
            # it should be a single chain
            assert len(item.provided_chain) == 1
            # alias
            if isinstance(res_target.val, ColumnRef):
                t, c = alias_dic.get_full_alias(res_target.val.fields)
                if res_target.name is None:
                    new_alias_dic.record_alias(c, t, item.provided_chain[0])
                else:
                    new_alias_dic.record_alias(res_target.name, t, item.provided_chain[0])
            else:
                new_alias_dic.record_alias(res_target.name, "", item.provided_chain[0])
            select_clause_list.append(item)
        select_clause = SelectClause(tuple(select_clause_list))

        # finalize
        if query.distinctClause is None:
            new_select_component = QueryComponent(select_clause=select_clause,
                                                  source_clause=from_clause_component,
                                                  where_clause=where_component,
                                                  having_clause=having_clause,
                                                  group_by_clause=group_by_component,
                                                  order_by_clause=order_by_component,
                                                  distinct_clause=NoneClause())
        else:
            new_select_component = QueryComponent(select_clause=select_clause,
                                                  source_clause=from_clause_component,
                                                  where_clause=where_component,
                                                  having_clause=having_clause,
                                                  group_by_clause=group_by_component,
                                                  order_by_clause=order_by_component,
                                                  distinct_clause=DistinctClause())

        return new_select_component, new_alias_dic

    if isinstance(query, RangeSubselect):
        from_clause_component, alias_dic = pglast_to_dsl(query.subquery, schema, alias_mapping,
                                                         complement_level=complement_level)
        assert isinstance(from_clause_component, QueryComponent)

        if query.alias is not None:
            # exposed space will be replaced
            new_alias_dic = AliasDic()
            for k, v in alias_dic.get_items():
                new_alias_dic.record_alias(k.column, query.alias.aliasname, v)
            return QueryComponentSource(from_clause_component, exact_alias=query.alias.aliasname), new_alias_dic
        else:
            new_alias_dic = alias_dic
            return QueryComponentSource(from_clause_component, alias_hint="cte"), new_alias_dic

    if isinstance(query, RangeVar):
        table_name = query.relname
        table = schema.get_table(table_name)
        table = Table(table_name, table.get_cols())

        provided_references = []
        table_component = TableClause(table)
        new_alias_dic = AliasDic()
        for c in table.table_cols:
            ref = ColChain(table, c[0], c[1])
            provided_references.append(ref)
            if query.alias is None:
                new_alias_dic.record_alias(c[0], table.table_name, ref)
            else:
                new_alias_dic.record_alias(c[0], query.alias.aliasname, ref)
        if query.alias is None:
            return TableClauseSource(table_component, alias_hint=table_name), new_alias_dic
        else:
            return TableClauseSource(table_component, exact_alias=query.alias.aliasname), new_alias_dic

    if isinstance(query, JoinExpr):
        # first deal with two tables
        larg_component, lalias_dic = pglast_to_dsl(query.larg, schema, alias_mapping, complement_level=complement_level)
        assert isinstance(larg_component, SourceClause)
        rarg_component, ralias_dic = pglast_to_dsl(query.rarg, schema, alias_mapping, complement_level=complement_level)
        assert isinstance(rarg_component, SourceClause)

        lalias_dic.combine(ralias_dic)

        if query.quals is None:
            raise TranslationNotSupportedException("currently enforce there's ON condition in JOIN")
        # then deal with on conditions
        on_component, alias_dic = pglast_to_dsl(query.quals, schema, lalias_dic, complement_level=complement_level)
        assert isinstance(on_component, PredicateClause)
        # finally deal with alias
        if query.alias is not None:
            new_alias_dic = AliasDic()
            for k, v in alias_dic.get_items():
                new_alias_dic.record_alias(k.column, query.alias.aliasname, v)
        else:
            new_alias_dic = alias_dic
        # depend on which kind of join it is, create different join components
        if query.jointype == JoinType.JOIN_LEFT:
            join_type = JoinTypeLeft()
        elif query.jointype == JoinType.JOIN_INNER:
            join_type = JoinTypeInner()
        elif query.jointype == JoinType.JOIN_RIGHT:
            join_type = JoinTypeLeft()
            larg_component, rarg_component = rarg_component, larg_component
        elif query.jointype == JoinType.JOIN_FULL:
            join_type = JoinTypeFull()
        else:
            raise "should't reach here - unrecognized join type"

        assert isinstance(join_type, JoinTypeClause)
        join_on_component = JoinWithOnClause(larg_component, rarg_component, join_type, on_component)

        return join_on_component, new_alias_dic

    if isinstance(query, A_Expr):
        larg, alias_dic = pglast_to_dsl(query.lexpr, schema, alias_mapping, complement_level=complement_level)
        assert isinstance(larg, SingleValueClause)
        rarg, alias_dic = pglast_to_dsl(query.rexpr, schema, alias_mapping, complement_level=complement_level)
        assert isinstance(rarg, SingleValueClause)
        if len(query.name) != 1:
            raise TranslationNotSupportedException("operator name length > 1 in expression?")

        # when entering here, there must be a context whether it is in WHERE or it is in HAVING.
        # depending on the context, we choose to create a WHEREOP component or HAVINGOP component
        # first deal with oprand
        match query.name[0].val:
            case "=":
                if complement_level % 2 == 0:
                    op = OpEq()
                else:
                    op = OpNeq()
            case "<":
                if complement_level % 2 == 0:
                    op = OpLt()
                else:
                    op = OpLeq()
                    larg, rarg = rarg, larg
            case "<=":
                if complement_level % 2 == 0:
                    op = OpLeq()
                else:
                    op = OpLt()
                    larg, rarg = rarg, larg
            case ">":
                if complement_level % 2 == 0:
                    op = OpLt()
                    larg, rarg = rarg, larg
                else:
                    op = OpLeq()

            case ">=":
                if complement_level % 2 == 0:
                    op = OpLeq()
                    larg, rarg = rarg, larg
                else:
                    op = OpLt()

            case "!=" | "<>":
                if complement_level % 2 == 0:
                    op = OpNeq()
                else:
                    op = OpEq()
            case _:
                raise "unknown oprand for expression"

        filter_component = PredicateOpClause(larg, op, rarg)
        return filter_component, alias_dic

    if isinstance(query, FuncCall):
        if len(query.args) != 1:
            raise TranslationNotSupportedException("currently we only support function with single argument")
        if len(query.funcname) != 1:
            raise TranslationNotSupportedException("currently we only support function with single name")

        match query.funcname[0].val.lower():
            case "sum":
                func_name = AggregationFuncNameSum()
            case "count":
                func_name = AggregationFuncNameCount()
            case "avg":
                func_name = AggregationFuncNameAvg()
            case "max":
                func_name = AggregationFuncNameMax()
            case "min":
                func_name = AggregationFuncNameMin()
            case "bit_and":
                func_name = AggregationFuncNameBitAnd()
            case "bit_or":
                func_name = AggregationFuncNameBitOr()
            case _:
                raise TranslationNotSupportedException("currently we only support function sum, count, avg")
        r, alias_dict = pglast_to_dsl(query.args[0], schema, alias_mapping, complement_level=complement_level)
        assert isinstance(r, ReferencableChain)
        if query.agg_distinct and query.agg_distinct == True:
            agg_component = AggregationClause(func_name, r, DistinctClause())
        else:
            agg_component = AggregationClause(func_name, r, NoneClause())

        #  deal with window function
        if query.over:
            over_clause = query.over
            assert isinstance(over_clause, WindowDef)
            if over_clause.partitionClause is not None:
                partition_clause_list: List[ReferencableChain] = []
                for item in over_clause.partitionClause:
                    item_chain, _ = pglast_to_dsl(item, schema, alias_mapping, complement_level=complement_level)
                    assert isinstance(item_chain, ReferencableChain)
                    partition_clause_list.append(item_chain)
                partition_clause = PartitionClauseList(tuple(partition_clause_list))
            else:
                partition_clause = NoneClause()
            if over_clause.orderClause is not None:
                order_by_clause_list: List[OrderByClauseItem] = []
                for i in over_clause.orderClause:
                    assert isinstance(i, SortBy)
                    sort_by_chain, _ = pglast_to_dsl(i.node, schema, alias_mapping, complement_level=complement_level)
                    assert isinstance(sort_by_chain, ReferencableChain)
                    tmp_config = i.sortby_dir
                    match tmp_config:
                        case SortByDir.SORTBY_DEFAULT | SortByDir.SORTBY_ASC:
                            sort_configuration = OrderByConfigurationASC()
                        case SortByDir.SORTBY_DESC:
                            sort_configuration = OrderByConfigurationDESC()
                        case _:
                            raise TranslationNotSupportedException("order by configuration not recognized!")
                    order_by_clause_list.append(OrderByClauseItem(sort_by_chain, sort_configuration))
                order_by_clause = OrderByClauseList(tuple(order_by_clause_list))
            else:
                order_by_clause = NoneClause()
            return WindowFuncCallClauseAgg(agg_func=agg_component,
                                           partition_clause=partition_clause,
                                           order_by_clause=order_by_clause), alias_dict
        return agg_component, alias_dict

    if isinstance(query, ColumnRef):
        r = alias_mapping.get_reference_by_alias(query.fields)
        return r, alias_mapping

    if isinstance(query, A_Const):
        match query.val:
            case String():
                value = StringConst(query.val.val)
            case Integer():
                value = IntConst(query.val.val)
            case Float():
                value = FloatConst(float(query.val.val))
            case _:
                raise TranslationNotSupportedException("not support this constant type")

        return value, alias_mapping

    if isinstance(query, BoolExpr):
        match query.boolop:
            case BoolExprType.OR_EXPR:
                if complement_level % 2 == 0:
                    logic_op = OpOr()
                else:
                    logic_op = OpAnd()
            case BoolExprType.AND_EXPR:
                if complement_level % 2 == 0:
                    logic_op = OpAnd()
                else:
                    logic_op = OpOr()
            case BoolExprType.NOT_EXPR:
                # deal with NOT BY some simple complementation
                inner, alias_dic = pglast_to_dsl(query.args[0], schema, alias_mapping,
                                                 complement_level=complement_level + 1)
                return inner, alias_dic
            case _:
                raise TranslationNotSupportedException("not recognize this binary op")

        if len(query.args) == 2:
            larg, alias_dic = pglast_to_dsl(query.args[0], schema, alias_mapping, complement_level=complement_level)
            assert isinstance(larg, PredicateClause)
            rarg, alias_dic = pglast_to_dsl(query.args[1], schema, alias_mapping, complement_level=complement_level)
            assert isinstance(rarg, PredicateClause)
            if isinstance(larg, NoneClause) and isinstance(rarg, NoneClause):
                new_component = NoneClause()
            elif isinstance(larg, NoneClause):
                new_component = rarg
            elif isinstance(rarg, NoneClause):
                new_component = larg
            else:
                new_component = PredicateBinOpLogicClause(larg, logic_op, rarg)
            return new_component, alias_mapping
        else:
            larg, alias_dic = pglast_to_dsl(query.args[0], schema, alias_mapping, complement_level=complement_level)
            rarg, alias_dic = pglast_to_dsl(BoolExpr(query.boolop, query.args[1:]), schema, alias_mapping,
                                            complement_level=complement_level)
            if isinstance(larg, NoneClause):
                new_component = rarg
            else:
                new_component = PredicateBinOpLogicClause(larg, logic_op, rarg)
            return new_component, alias_mapping

    if isinstance(query, SubLink):
        return NoneClause(), alias_mapping
    if isinstance(query, NullTest):
        return NoneClause(), alias_mapping

    raise TranslationNotSupportedException("not supported for " + query.__class__.__name__)
