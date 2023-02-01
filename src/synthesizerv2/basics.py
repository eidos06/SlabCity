from pglast.ast import *
from enum import IntEnum, auto
from typing import List
import pickle
import uuid


class SQLFunction(IntEnum):
    Max = auto()
    Min = auto()
    Count = auto()
    Avg = auto()
    Sum = auto()


class SQLExpressionFunc(IntEnum):
    Add = auto()


class BaseColumn:
    def __init__(self, table, colname):
        self.table = table
        self.colname = colname


class TableSchema:
    def __init__(self, table_name, table_col_names):
        self.table_name = table_name
        self.table_col_names = table_col_names

    def get_full_col_names(self):
        return [self.table_name + "." + col_name for col_name in self.table_col_names]

    def get_col_names(self):
        return self.table_col_names

    def get_base_columns(self):
        result = []
        for col_name in self.table_col_names:
            result.append(BaseColumn(self.table_name, col_name))
        return result


class TableSchemaEnv:
    def __init__(self):
        self.schemas = {}

    def add_schema(self, schema: TableSchema):
        self.schemas[schema.table_name] = schema
        return self

    def find_schema(self, table_name: str) -> TableSchema:
        return self.schemas[table_name]

    def get_table_names(self):
        return list(self.schemas.keys())


class QueryWithSchema:
    def __init__(self, schema: TableSchemaEnv, query: Node):
        self.schema: TableSchemaEnv = schema
        self.query: Node = query


class HoleAnalyzeResult:
    def __init__(self, at_outmost_level):
        self.at_outmost_level = at_outmost_level


class SynthesisEnv:
    def __init__(self, columns_in_scope, fresh_counter, table_schema_env: TableSchemaEnv,
                 possible_function_candidate, possible_expression_candidate,
                 possible_constants_candidate, hole_info, num_output_columns):
        self.columns_in_scope: List[FreshCol] = columns_in_scope
        self.fresh_counter: FreshColCounterEnv = fresh_counter
        self.table_schema: TableSchemaEnv = table_schema_env
        self.possible_function_candidate: List[SQLFunction] = possible_function_candidate
        self.possible_expression_candidate: List[SQLExpressionFunc] = possible_expression_candidate
        self.possible_constants_candidate = possible_constants_candidate
        self.hole_info = hole_info
        self.num_output_columns = num_output_columns


class FreshColCounterEnv:
    def __init__(self, initial_counter=0):
        self.fresh_var_counter = initial_counter

    def get_fresh_col(self):
        self.fresh_var_counter += 1
        return FreshCol(self.fresh_var_counter)


class CteNameCounterEnv:
    def __init__(self):
        self.cte_counter = 0

    def get_fresh_cte_name(self):
        self.cte_counter += 1
        return "cte" + str(self.cte_counter)


def deepcopy(item):
    copier = getattr(item, "__deepcopy__", None)
    if copier is not None:
        return copier(item)
    else:
        return pickle.loads(pickle.dumps(item))
    # return copy.deepcopy(item)
