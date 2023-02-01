from testing.constrained_generation import Constrained_Random_Table_Generation
from testing.util import extract_query_info, QueryTestingInfo

import sys
sys.path.append('../..')
from synthesizerv2.basics import TableSchema, TableSchemaEnv

class Heuristic_Random_Table_Generation(Constrained_Random_Table_Generation):
    def __init__(self, schema: dict, sizes, query, fake=None, raw_cstr: dict = {}, seed=2333, is_extreme: bool=False, enable_group_by: bool=False) -> None:
        super().__init__(schema, sizes, fake, raw_cstr, seed, is_extreme)
        tables = TableSchemaEnv()
        for table in schema['Tables']:
            table_name = table['TableName'].lower()
            columns = []
            for column in table["PKeys"]:
                columns.append(column["Name"].lower())
            for column in table["FKeys"]:
                if "Name" in column:
                    columns.append(column["Name"].lower())
                elif "FName" in column:
                    columns.append(column["FName"].lower())
            for column in table["Others"]:
                columns.append(column["Name"].lower())
            tables.add_schema(TableSchema(table_name, columns))
        filters = extract_query_info(query, tables)
        
        # debug output
        # print(self.cstr_d)

        # parse filters into constraints
        for ft in filters:
            # debug output
            # print(f"get filter {ft.lexpr} {ft.operator} {ft.rexpr}")
            assert isinstance(ft, QueryTestingInfo)
            if ft.type == "groupby" and enable_group_by:
                if ft.operator == "unique":
                    is_covered = False
                    col_id = self.col2id[ft.lexpr]
                    if 'unique' not in self.cstr_d:
                        self.cstr_d['unique'] = []
                    for i, u_cols in enumerate(self.cstr_d['unique']):
                        if col_id in u_cols:
                            self.cstr_d['unique'][i] = [col_id]
                            is_covered = True
                    if not is_covered:
                        self.cstr_d['unique'].append([col_id])
            elif ft.type == 'join':
                # check if the join rel is covered by fkey rel
                is_covered = False
                lcol_id = self.col2id[ft.lexpr]
                rcol_id = self.col2id[ft.rexpr]
                for cstr in self.cstr_d[lcol_id]:
                    if cstr['operator'] == 'subset':
                        if cstr['parent_col_id'] == rcol_id:
                            is_covered = True
                        else:
                            rcol_id = cstr['parent_col_id']
                        break
                for cstr in self.cstr_d[rcol_id]:
                    if cstr['operator'] == 'subset':
                        if cstr['parent_col_id'] == lcol_id:
                            is_covered = True
                        else:
                            lcol_id = cstr['parent_col_id']
                        break
                if is_covered:
                    continue
                else:
                    if 'join' not in self.cstr_d:
                        self.cstr_d['join'] = []
                    self.cstr_d['join'].append([
                        lcol_id,
                        rcol_id
                    ])
            elif ft.type == 'pred':
                # print(ft)
                value = None
                if ft.rexpr.isnumeric():
                    value = ft.rexpr
                elif 'E+' in ft.rexpr:
                    value = str(int(float(ft.rexpr)))
                elif '-' in ft.rexpr and '->' not in ft.rexpr:
                    value = str(ft.rexpr.strip())
                elif '.' not in ft.rexpr:
                    value = str(ft.rexpr)
                if value != None:
                    # check agg functionsoft
                    is_skip = False
                    if '->' in ft.lexpr:
                        col, agg = ft.lexpr.split('->')
                        col = col.strip()
                        col_id = self.col2id[col]
                        agg = agg.strip()
                        if agg not in ['max', 'min', 'sum']:
                            is_skip = True
                            if agg == "count":
                                is_covered = False
                                if 'unique' not in self.cstr_d:
                                    self.cstr_d['unique'] = []
                                for i, u_cols in enumerate(self.cstr_d['unique']):
                                    if col_id in u_cols:
                                        if len(u_cols) == 2 and str(ft.rexpr).isnumeric():
                                            if ft.operator == '>':
                                                num = int(ft.rexpr) + 1
                                            else:
                                                num = int(ft.rexpr)
                                            self.cstr_d['unique'][i] = u_cols + [f"{col_id}-{num}"]
                                            pass
                    else:
                        col_id = self.col2id[ft.lexpr.strip()]
                    if not is_skip:
                        if ft.operator in ['>', '>=']:
                            self.cstr_d[col_id].append({
                                "parent_col_id": -1,
                                "operator": "range",
                                "is_soft": True,
                                "info": {
                                    "discrete": False,
                                    "lower": value,
                                    "upper": None
                                }
                            })
                        elif ft.operator in ['<', '<=']:
                            self.cstr_d[col_id].append({
                                "parent_col_id": -1,
                                "operator": "range",
                                "is_soft": True,
                                "info": {
                                    "discrete": False,
                                    "lower": None,
                                    "upper": value
                                }
                            })
                        elif ft.operator == '=':
                            self.cstr_d[col_id].append({
                                "parent_col_id": -1,
                                "operator": "=",
                                "is_soft": True,
                                "info": {
                                    "value": [value]
                                }
                            })
                        elif ft.operator == '<>':
                            self.cstr_d[col_id].append({
                                "parent_col_id": -1,
                                "operator": "!=",
                                "is_soft": True,
                                "info": {
                                    "value": [value]
                                }
                            })
                else:
                    if ft.operator == '=':
                        if '->' in ft.lexpr or '->' in ft.rexpr:
                            # print('not support agg')
                            pass
                        else:
                            lcol_id = self.col2id[ft.lexpr]
                            rcol_id = self.col2id[ft.rexpr]
                            # check if = is covered by subset
                            is_covered = False
                            for cstr in self.cstr_d[lcol_id]:
                                if cstr['operator'] == 'subset' and cstr['parent_col_id'] == rcol_id:
                                    is_covered = True
                            for cstr in self.cstr_d[rcol_id]:
                                if cstr['operator'] == 'subset' and cstr['parent_col_id'] == lcol_id:
                                    is_covered = True
                            if not is_covered:
                                self.cstr_d[lcol_id].append({
                                    "parent_col_id": rcol_id,
                                    "operator": "=",
                                    "is_soft": True
                                })
                                self.cstr_d[rcol_id].append({
                                    "parent_col_id": lcol_id,
                                    "operator": "=",
                                    "is_soft": True
                                })
        
        # debug output
        # print(self.cstr_d)