from collections import defaultdict
from copy import deepcopy
import logging
import sys
from itertools import permutations
from collections import Counter
sys.path.append('../..')
from synthesizerv2.analysis import analyze_base_table_and_columns, analyze_join_conditions, extract_filters
from synthesizerv2.basics import QueryWithSchema, TableSchemaEnv
from synthesizerv2.translator import translate
from synthesizerv2.filter import *
from pglast.parser import parse_sql
import pglast

schema_id = 0

def get_schema_name() -> str:
    global schema_id
    schema_id += 1
    return f"table_{schema_id}"

def dif_table(data1, data2, consider_order=False):
    if len(data1) == 0 and len(data2) == 0:
        return False
    def generate_permutation_datas(data):
        result = []
        if len(data) == 0:
            return []
        data_permutations = []
        for i in data:
            row_permutation = list(permutations(i))
            data_permutations.append(row_permutation)
        row_permutation_len = len(data_permutations[0])
        for i in range(row_permutation_len):
            new_permutation_table = []
            for row in data_permutations:
                new_permutation_table.append(row[i])
            result.append(new_permutation_table)
        return result

    if len(data1) != len(data2):
        return True
    data2_permutations = generate_permutation_datas(data2)

    if consider_order:
        for d2 in data2_permutations:
            if d2 == data1:
                return False
    else:
        bag_semantic_data1 = dict(Counter(data1))
        for d2 in data2_permutations:
            bag_semantic_data2 = dict(Counter(d2))
            if bag_semantic_data1 == bag_semantic_data2:
                return False
    return True

def topo_sort_cols(num_col, cstr: dict):
    """
    Gives a reversed topological sorting of tables according to foreign key dependency
    Raises an exception when there is a foreign key dependency cycle
    """
    n = num_col
    edges = defaultdict(list)
    alternatives = []
    chosen_alternatives = []
    for col_id in cstr.keys():
        if col_id in ['unique', 'join']: 
            continue
        for col_cstr in cstr[col_id]:
            if col_cstr['parent_col_id'] != -1:
                if col_cstr['operator'] in ['>', '<', '!=', '=']:
                    if [col_id, col_cstr['parent_col_id']] not in alternatives \
                    and [col_cstr['parent_col_id'], col_id] not in alternatives:
                        alternatives.append([
                            col_id, col_cstr['parent_col_id']
                            ])
                else:
                    edges[col_id].append(col_cstr['parent_col_id'])
    if 'unique' in cstr.keys():
        for unique_rel in cstr['unique']:
            if len(unique_rel) == 2:
                if edges[unique_rel[0]] == []:
                    edges[unique_rel[0]] = [unique_rel[1]]
    def dfs(x):
        visited[x] = 1
        for y in use_edges[x]:
            if visited[y] == 0:
                dfs(y)
            elif visited[y] == 1:
                raise Exception("Foreign key dependency cycle")
        visited[x] = 2
        topo_sorted.append(x)
    success = False
    if len(alternatives) == 0:
        use_edges = deepcopy(edges)
        topo_sorted = []
        visited = [0 for _ in range(n)]
        for k in range(n):
            try:
                if visited[k] == False:
                    dfs(k)
                if k == n-1:
                    success = True
            except Exception as e:
                    logging.info(f"fail to topo-sort using alternative {i}")
                    break
        
        if success:
            return topo_sorted
    else:
        for i in range(0, 2 ** len(alternatives)):
            use_edges = deepcopy(edges)
            bin_str = bin(i).split('b')[-1]
            bin_str = '0' * (len(alternatives) - len(bin_str)) + bin_str
            chosen_alternatives = []
            for j, d_str in enumerate(bin_str):
                decision = int(d_str)
                if decision == 0:
                    chosen_alternatives.append(alternatives[j])
                else:
                    chosen_alternatives.append([alternatives[j][-1], alternatives[j][0]])
            # print(chosen_alternatives)
            for choice in chosen_alternatives:
                use_edges[choice[0]].append(choice[1])
            topo_sorted = []
            visited = [0 for _ in range(n)]
            for k in range(n):
                try:
                    if visited[k] == False:
                        dfs(k)
                    if k == n-1:
                        success = True
                except Exception as e:
                        logging.info(f"fail to topo-sort using alternative {i}")
                        break
            if success:
                return topo_sorted
    if not success:
        logging.info(f"fail to topo sort the dependency")
        raise Exception('Detected circular dependency')

def print_db(database: list):
    # for table in database:
    #     keys = []
    #     for key in table.keys():
    #         keys.append(key)
    #     print(",".join(keys))
    #     values = []
    #     for key in keys:
    #         values.append(table[key])
    #     values = list(map(list, zip(*values)))
    #     for row in values:
    #         row_str = []
    #         for entry in row:
    #             row_str.append(str(entry))
    #         print(",".join(row_str))
    #     print("-"*50)
    return

def readable_db(database: list):
    output = ""
    for table in database:
        keys = []
        for key in table.keys():
            keys.append(key)
        output += ",".join(keys) + "\n"
        values = []
        for key in keys:
            values.append(table[key])
        values = list(map(list, zip(*values)))
        for row in values:
            row_str = []
            for entry in row:
                row_str.append(str(entry))
            output += ",".join(row_str) + '\n'
        output += "-"*50 + "\n"
    return output


class QueryTestingInfo():
    def __init__(self, _type, _operator, _lexpr, _rexpr) -> None:
        self.type = str(_type)
        self.operator = str(_operator)
        self.lexpr = str(_lexpr)
        self.rexpr = str(_rexpr)
    
    def is_dup(self, info) -> bool:
        if self.type != info.type:
            return False
        if self.operator == info.operator:
            if self.lexpr == info.lexpr and self.rexpr == info.rexpr:
                return True
            return False
        if self.operator in ['>', '<'] and info.operator in ['<', '>']:
            if self.lexpr == info.rexpr and self.rexpr == info.lexpr:
                return True
            return False
        if self.operator in ['>=', '<='] and info.operator in ['<=', '>=']:
            if self.lexpr == info.rexpr and self.rexpr == info.lexpr:
                return True
            return False
        if self.operator in ['=', '<>'] and info.operator in ['=', '<>']:
            if self.lexpr == info.lexpr and self.rexpr == info.rexpr:
                return True
            return False
        return False
    
    def __str__(self) -> str:
        return f"{self.lexpr} {self.operator} {self.rexpr}"
        


def extract_query_info(query: str, schema: TableSchemaEnv):
    query = parse_sql(query)[0].stmt
    query = translate(query, schema)   
    q = QueryWithSchema(schema, query)
    mapping = analyze_base_table_and_columns(q)
    info_conditions = analyze_join_conditions(q.query, mapping)
    info_filter = extract_filters(q, info_conditions, mapping)
    useful_filters = []
    for ft in info_filter:
        if isinstance(ft[0], Predicate_Filter):
            # useful_filters.append(['predicate', ft[0]])
            operator = ft[0].content.name[0].val
            lexpr = ft[0].content.lexpr
            rexpr = ft[0].content.rexpr
            lexpr = lexpr.val.val if isinstance(lexpr, pglast.ast.A_Const) else lexpr
            rexpr = rexpr.val.val if isinstance(rexpr, pglast.ast.A_Const) else rexpr
            if str(lexpr) != str(rexpr):
                info = QueryTestingInfo('pred', operator, lexpr, rexpr)
                is_dup = False
                for uft in useful_filters:
                    if info.is_dup(uft):
                        is_dup = True
                if not is_dup:
                    useful_filters.append(info)
        elif isinstance(ft[0], Join_Condition_Filter):
            # useful_filters.append(['join', ft[0]])
            operator = ft[0].content.name[0].val
            if operator == '=':
                lexpr = ft[0].content.lexpr
                rexpr = ft[0].content.rexpr
                lexpr = lexpr.val.val if isinstance(lexpr, pglast.ast.A_Const) else lexpr
                rexpr = rexpr.val.val if isinstance(rexpr, pglast.ast.A_Const) else rexpr
                if lexpr != rexpr:
                    info = QueryTestingInfo('join', operator, lexpr, rexpr)
                    is_dup = False
                    for uft in useful_filters:
                        if info.is_dup(uft):
                            is_dup = True
                    if not is_dup:
                        useful_filters.append(info)
        elif isinstance(ft[0], Group_Filter):
            for column in ft[0].content:
                info = QueryTestingInfo('groupby', 'unique', column, None)
                is_dup = False
                for uft in useful_filters:
                    if info.is_dup(uft):
                        is_dup = True
                if not is_dup:
                    useful_filters.append(info)
        else:
            raise Exception(f"unsupported filter {ft[0]}")
    return useful_filters

# class Column_Schema():
#     def __init__(self, _name, _type, _isPkey, _enums=[], fkey_info="") -> None:
#         self.name = str(_name).lower()
#         if _type not in ['int', 'numeric', 'varchar', 'enum', 'fkey']:
#             raise Exception(f"unsupported type {_type}")
#         else:
#             self.type = _type
#             if self.type == 'enum':
#                 self.enums = _enums
#             if self.type == 'fkey':
#                 self.ftabcol = fkey_info
#         self.isPkey = _isPkey

# class Table_Schema():
#     def __init__(self, _name, _cols: list) -> None:
#         self.name = str(_name).lower()
#         self.cols = _cols

# class Database_Schema():
#     def __init__(self) -> None:
#         self.tabs = {}
#         self.cols = []
#         self.col2id = {}
#         self.connection = {}
    
#     def parse_schema(self, schema: dict):
#         tab_names = []
#         for table in schema['Tables']:
#             tab_name = table['TableName'].lower()
#             tab_names.append(tab_name)
#         for table in schema['Tables']:
#             tab_name = table['TableName'].lower()
#             cols = []
#             for pkey in table['PKeys']:
#                 col_name = pkey['Name'].lower()
#                 col_type = pkey['Type']
#                 col_schema = Column_Schema(col_name, col_type, True)
#                 self.col2id[f'{tab_name}.{col_name}'] = len(self.cols)
#                 self.cols.append(col_schema)
#                 cols.append(col_schema)
#             for fkey in table['FKeys']:
#                 col_name = fkey['FName']
#                 pcol_name = fkey['PName']
#                 ptab_id = int(fkey['PTable'])
#                 ptab_name = tab_names[ptab_id]
#                 col_schema = Column_Schema(col_name, col_type, False, f'{ptab_name}.{pcol_name}')
#                 self.col2id[f'{tab_name}.{col_name}'] = len(self.cols)
#                 self.cols.append(col_schema)
#             for other in table['Others']:
#                 col_name = other['Name']
#                 col_type = other['Type']
#                 self.col2id[f'{tab_name}.{col_name}'] = len(self.cols)
#                 self.cols.append(col_schema)
#                 cols.append(col_schema)
#             table_schema = Table_Schema(tab_name, cols)
#             self.tabs[tab_name] = table_schema
#         # establish pkey relation
        

#     def topo_sort(self):

