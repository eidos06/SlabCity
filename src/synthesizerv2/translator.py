from pglast.ast import *
from typing import Tuple
from synthesizerv2.basics import *
from copy import deepcopy

class AliasEnv:
    def __init__(self):
        self.column_alias = {}
        self.table_alias = {}

    def add_column_alias(self, column_name, alias):
        self.column_alias[alias] = column_name
        return self

    def find_column_by_alias(self, alias):
        while alias in self.column_alias.keys():
            alias = self.column_alias[alias]
        return alias

    def add_table_alias(self, table_name, alias):
        self.table_alias[alias] = table_name
        return self

    def find_table_by_alias(self, alias):
        while alias in self.table_alias.keys():
            alias = self.table_alias[alias]
        return alias


class TranslationEnv:
    def __init__(self):
        self.col_mapping = {}
        self.reverse_mapping = {}

    def add_mapping(self, origin, new):
        self.col_mapping[origin] = new
        self.reverse_mapping[new] = origin
        return self

    def get_mapping(self, origin):
        return self.col_mapping[self.get_full_col_name(origin)]

    def get_origin(self, new):
        while new in self.reverse_mapping.keys():
            new = self.reverse_mapping[new]
        return new

    def apply_table_alias(self, alias):
        new_mapping = {}
        new_reverse_mapping = {}
        for key, value in self.col_mapping.items():
            if isinstance(key, str):
                if key.find(".") != -1:
                    key = alias + key[key.find("."):]
                else:
                    key = alias + "." + key
                new_mapping[key] = value
                new_reverse_mapping[value] = key
        self.col_mapping = new_mapping
        self.reverse_mapping = new_reverse_mapping
        return self

    def get_full_col_name(self, col_name):
        if col_name in self.col_mapping:
            return col_name

        for key in self.col_mapping.keys():
            if isinstance(key, str):
                if key.find(".") != -1:
                    if key[key.find(".") + 1:] == col_name:
                        return key
        raise Exception("undefined reference: " + col_name)

    def apply_column_alias(self, origin, alias):
        new_mapping = {}
        new_reverse_mapping = {}
        for key, value in self.col_mapping.items():
            if key == origin:
                key = alias
            new_mapping[key] = value
            new_reverse_mapping[value] = key
        self.col_mapping = new_mapping
        self.reverse_mapping = new_reverse_mapping
        return self

    def join(self, other):
        self.col_mapping.update(other.col_mapping)
        self.reverse_mapping.update(other.reverse_mapping)
        return self


def translate(node, table_schemas: TableSchemaEnv) \
        -> Node:
    node_copied = deepcopy(node)
    result, _, _, _ = __translate(node_copied, FreshColCounterEnv(), table_schemas, TranslationEnv())
    return result


def __translate(node, counter_env: FreshColCounterEnv, table_schemas: TableSchemaEnv, translation_env: TranslationEnv) \
        -> Tuple[Node, FreshColCounterEnv, TableSchemaEnv, TranslationEnv]:
    counter_env, table_schemas, translation_env = deepcopy(counter_env), deepcopy(table_schemas), deepcopy(
        translation_env)

    if isinstance(node, RangeSubselect):
        (rename, counter_env1, table_schemas1, translation_env1) = __translate(node.subquery,
                                                                             counter_env,
                                                                             table_schemas,
                                                                             translation_env
                                                                             )
        if node.alias:
            translation_env1.apply_table_alias(node.alias.aliasname)
        return rename, counter_env1, table_schemas1, translation_env1

    if isinstance(node, SortBy):
        (new_col, counter_env1, table_schemas1, translation_env1) = __translate(node.node,
                                                                              counter_env,
                                                                              table_schemas,
                                                                              translation_env)
        return SortBy(node=new_col, sortby_dir=node.sortby_dir, sortby_nulls=node.sortby_nulls,
                      useOp=node.useOp), counter_env1, table_schemas1, translation_env1

    if isinstance(node, SelectStmt):
        (rename, counter_env1, table_schemas1, translation_env1) = __translate(node.fromClause[0],
                                                                             counter_env,
                                                                             table_schemas,
                                                                             translation_env)
        node.fromClause = tuple([rename])

        if node.groupClause:
            new_group_list = []
            for group_item in node.groupClause:
                # group by number
                if isinstance(group_item, A_Const):
                    if isinstance(group_item.val, Integer):
                        converted_group_item = node.targetList[group_item.val.val-1].val
                else:
                    converted_group_item = group_item
                (new_group_item, counter_env1, table_schemas1, translation_env1) = __translate(converted_group_item,
                                                                                             counter_env1,
                                                                                             table_schemas1,
                                                                                             translation_env1)
                new_group_list.append(new_group_item)
            node.groupClause = tuple(new_group_list)

        if node.havingClause:
            (new_having, counter_env1, table_schemas1, translation_env1) = __translate(node.havingClause,
                                                                                     counter_env1,
                                                                                     table_schemas1,
                                                                                     translation_env1)
            node.havingClause = new_having

        if node.whereClause:
            (new_where, counter_env1, table_schemas1, translation_env1) = __translate(node.whereClause,
                                                                                    counter_env1,
                                                                                    table_schemas1,
                                                                                    translation_env1)
            node.whereClause = new_where

        if node.sortClause:
            new_sort_list = []
            for sortby in node.sortClause:
                if isinstance(sortby.node, A_Const):
                    if isinstance(sortby.node.val, Integer):
                        real_node = node.targetList[sortby.node.val.val-1].val
                        converted_sortby = deepcopy(sortby)
                        converted_sortby.node = real_node
                    else:
                        raise Exception("not valid SortBy clause - must be an integer or column reference, got " + type(sortby.node.val).__name__ + str(sortby.node.val))
                else:
                    converted_sortby = sortby
                (new_sort_by, counter_env1, table_schemas1, translation_env1) = __translate(converted_sortby,
                                                                                          counter_env1,
                                                                                          table_schemas1,
                                                                                          translation_env1)
                new_sort_list.append(new_sort_by)
            node.sortClause = tuple(new_sort_list)

        new_col_list = []
        for target in node.targetList:
            (new_target, counter_env1, table_schemas1, translation_env1) = __translate(target,
                                                                                     counter_env1,
                                                                                     table_schemas1,
                                                                                     translation_env1)
            new_col_list.append(new_target)
        node.targetList = tuple(new_col_list)

        new_cols = []
        new_translation_env = TranslationEnv()
        for t in node.targetList:
            if isinstance(t, ResTarget):
                col_name = t.val
                fresh_col = counter_env1.get_fresh_col()
                new_cols.append(fresh_col)
                new_translation_env.add_mapping(translation_env1.get_origin(col_name), fresh_col)
            elif isinstance(t, Rename):
                col_name = t.list_variables[0]
                fresh_col = counter_env1.get_fresh_col()
                new_cols.append(fresh_col)
                new_translation_env.add_mapping(translation_env1.get_origin(col_name), fresh_col)
            else:
                raise Exception("unsupported type in targetList before renaming.")

        return Rename(new_cols, node), counter_env1, table_schemas1, new_translation_env

    if isinstance(node, RangeVar):
        origin_table_name = node.relname
        schema: TableSchema = table_schemas.find_schema(origin_table_name)
        col_names = schema.get_full_col_names()
        fresh_cols = []
        for name in col_names:
            fresh_col = counter_env.get_fresh_col()
            fresh_cols.append(fresh_col)
            translation_env.add_mapping(name, fresh_col)
        if node.alias:
            translation_env.apply_table_alias(node.alias.aliasname)
            node.alias = None
        return Rename(fresh_cols, node), counter_env, table_schemas, translation_env

    if isinstance(node, JoinExpr):

        (rename1, counter_env1, table_schemas1, translation_env1) = __translate(node.larg,
                                                                              counter_env,
                                                                              table_schemas,
                                                                              translation_env)
        (rename2, counter_env2, table_schemas2, translation_env2) = __translate(node.rarg,
                                                                              counter_env1,
                                                                              table_schemas,
                                                                              translation_env)
        translation_env2.join(translation_env1)

        (quals, _, _, _) = __translate(node.quals,
                                     counter_env2, table_schemas2, translation_env2)

        if not isinstance(rename1, Rename) or not isinstance(rename2, Rename):
            raise Exception("problem occured in translating JoinExpr - expecting two renames in larg and rarg.")

        list_old_variables = rename1.list_variables + rename2.list_variables
        fresh_cols = []
        for old_var in list_old_variables:
            fresh_col = counter_env2.get_fresh_col()
            translation_env2.add_mapping(translation_env2.get_origin(old_var), fresh_col)
            fresh_cols.append(fresh_col)

        node.larg = rename1
        node.rarg = rename2
        node.quals = quals
        return Rename(fresh_cols, node), counter_env2, table_schemas2, translation_env2

    if isinstance(node, ColumnRef):
        if len(node.fields) == 2:
            table_name = node.fields[0].val
            column_name = node.fields[1].val
            return (translation_env.get_mapping(table_name.lower() + "." + column_name.lower()),
                    counter_env,
                    table_schemas,
                    translation_env)
        elif len(node.fields) == 1:
            return (translation_env.get_mapping(node.fields[0].val),
                    counter_env,
                    table_schemas,
                    translation_env)
        else:
            raise Exception("unsupported ColumnRef")

    if isinstance(node, A_Expr):
        (l, _, _, _) = __translate(node.lexpr,
                                 counter_env,
                                 table_schemas,
                                 translation_env)
        (r, _, _, _) = __translate(node.rexpr,
                                 counter_env,
                                 table_schemas,
                                 translation_env)
        node.lexpr = l
        node.rexpr = r
        return node, counter_env, table_schemas, translation_env

    if isinstance(node, ResTarget):
        (new_val, counter_env1, table_schemas1, translation_env1) = __translate(node.val, counter_env, table_schemas,
                                                                              translation_env)
        if isinstance(node.val, ColumnRef):
            if node.name is not None:
                if isinstance(node.val, ColumnRef):
                    if len(node.val.fields) == 2:
                        translation_env1.apply_column_alias(
                            node.val.fields[0].val.lower() + "." + node.val.fields[1].val.lower(),
                            node.name)
                    elif len(node.val.fields) == 1:
                        translation_env1.apply_column_alias(
                            translation_env1.get_full_col_name(node.val.fields[0].val.lower()),
                            node.name)
                    else:
                        raise Exception("Some errors happen")
            return ResTarget(val=new_val), counter_env1, table_schemas1, translation_env1

        translation_env1.add_mapping(node.name,
                                     node.val)
        return ResTarget(val=new_val), counter_env1, table_schemas1, translation_env1

    if isinstance(node, BoolExpr):
        new_expression_list = []
        for item in node.args:
            (new_item, alias_env1, table_schemas1, translation_env1) = __translate(item,
                                                                                 counter_env,
                                                                                 table_schemas,
                                                                                 translation_env)
            new_expression_list.append(new_item)
        node.args = tuple(new_expression_list)
        return node, counter_env, table_schemas, translation_env

    if isinstance(node, FuncCall):
        new_args = []
        for arg in node.args:
            (new_val, _, _, _) = __translate(arg, counter_env, table_schemas,
                                           translation_env)
            new_args.append(new_val)
        node.args = tuple(new_args)
    return node, counter_env, table_schemas, translation_env


def _reverse_translate_helper(query: Node, table_schemas: TableSchemaEnv,
                              cte_name_counter: CteNameCounterEnv = CteNameCounterEnv(),
                              ) -> Tuple[Node, CteNameCounterEnv]:
    if isinstance(query, Rename):
        if isinstance(query.query, RangeVar):
            # Rename([c1, c2, c3], T1) -> SELECT ? AS c1, ? as c2, ? as c3 FROM T1
            target_lists = []
            for idx, fresh_col in enumerate(query.list_variables):
                target_lists.append(ResTarget(name=str(fresh_col), val=ColumnRef(
                    fields=[table_schemas.find_schema(query.query.relname).table_col_names[idx]])))
            return RangeSubselect(subquery=SelectStmt(targetList=target_lists, fromClause=tuple([query.query])),
                                  alias=cte_name_counter.get_fresh_cte_name()), cte_name_counter

        if isinstance(query.query, SelectStmt):
            # Rename([c1, c2, c3], SELECT ?, ?, ? FROM ...) -> SELECT ? as c1, ? as c2, ? as c2 FROM reverse_translate(...)
            target_lists = []
            for idx, fresh_col in enumerate(query.list_variables):
                query.query.targetList[idx].name = str(fresh_col)
            translated_inner, cte_name_counter = _reverse_translate_helper(query.query.fromClause[0], table_schemas)
            query.query.fromClause = tuple([translated_inner])
            return RangeSubselect(subquery=query.query, alias=cte_name_counter.get_fresh_cte_name()), cte_name_counter

        if isinstance(query.query, JoinExpr):
            target_lists = []
            old_columns = query.query.larg.list_variables + query.query.rarg.list_variables
            for idx, fresh_col in enumerate(query.list_variables):
                target_lists.append(ResTarget(name=str(fresh_col),
                                              val=ColumnRef(fields=[str(old_columns[idx])])))
            query.query.larg, cte_name_counter = _reverse_translate_helper(query.query.larg, table_schemas,
                                                                           cte_name_counter)
            query.query.rarg, cte_name_counter = _reverse_translate_helper(query.query.rarg, table_schemas,
                                                                           cte_name_counter)
            return RangeSubselect(subquery=SelectStmt(targetList=target_lists, fromClause=tuple([query.query])),
                                  alias=cte_name_counter.get_fresh_cte_name()), cte_name_counter
    if isinstance(query, SelectStmt):
        #  SELECT ?, ?, ? FROM ... -> SELECT ? as c1, ? as c2, ? as c2 FROM reverse_translate(...)
        target_lists = []
        translated_inner, cte_name_counter = _reverse_translate_helper(query.fromClause[0], table_schemas)
        query.fromClause = tuple([translated_inner])
        return RangeSubselect(subquery=query, alias=cte_name_counter.get_fresh_cte_name()), cte_name_counter
    raise Exception("unsupported reverse translation type")


def reverse_translate(query: Node, table_schemas: TableSchemaEnv
                      ) -> Node:
    copied_query = deepcopy(query)
    result, _ = _reverse_translate_helper(copied_query, table_schemas, CteNameCounterEnv())
    return result.subquery
