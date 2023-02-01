class Parser():
    def __init__(self, schema: dict, constraints: dict={}) -> None:
        self.schema = schema
        self.cstr_raw_dict = constraints
        self.cstr_d = None
    
    def gen_col_dicts(self, sizes: list):
        '''
        generate two dicts to query a col id or query a col name
        based on default table schema
        store and return the generated dicts
        all characters are converted to lower case
        '''
        if len(sizes) < len(self.schema['Tables']):
            raise ValueError('Num of sizes is invalid')
        unique_id = 0
        self.col2id = {}
        self.id2col = {}
        for table_schema, size in zip(self.schema['Tables'], sizes):
            tab_name = table_schema['TableName'].lower()
            pkeys = table_schema['PKeys']
            fkeys = table_schema['FKeys']
            others = table_schema['Others']
            if len(pkeys) != 0:
                for pkey in pkeys:
                    self.id2col[unique_id] = {
                        'table_name': tab_name,
                        'column_name': pkey['Name'].lower(),
                        'size': size,
                        'type': pkey["Type"]
                    }
                    tabcol = tab_name + '.' + pkey['Name'].lower()
                    self.col2id[tabcol] = unique_id
                    unique_id += 1
            if len(fkeys) != 0:
                is_pkey = False
                for fkey in fkeys:
                    for pkey in pkeys:
                        if fkey['FName'] == pkey['Name']:
                            is_pkey = True
                    if is_pkey:
                        continue
                    self.id2col[unique_id] = {
                        'table_name': tab_name,
                        'column_name': fkey['FName'].lower(),
                        'size': size,
                        'type': pkey["Type"]
                    }
                    tabcol = tab_name + '.' + fkey['FName'].lower()
                    self.col2id[tabcol] = unique_id
                    unique_id += 1
            if len(others) != 0:
                for other in others:
                    self.id2col[unique_id] = {
                        'table_name': tab_name,
                        'column_name': other['Name'].lower(),
                        'size': size,
                        'type': other["Type"]
                    }
                    tabcol = tab_name + '.' + other['Name'].lower()
                    self.col2id[tabcol] = unique_id
                    unique_id += 1
        return self.id2col, self.col2id


    def gen_schema_constraints(self):
        '''
        generate constraints based on the pkey and fkey info in schema
        '''
        if self.cstr_d == None:
            self.cstr_d = {}
        for table_schema in self.schema['Tables']:
            tab_name = table_schema['TableName'].lower()
            pkeys = table_schema['PKeys']
            fkeys = table_schema['FKeys']
            if len(pkeys) != 0:
                unique_cols = []
                for pkey in pkeys:
                    tabcol = tab_name + '.' + pkey['Name'].lower()
                    col_id = self.col2id[tabcol]
                    unique_cols.append(col_id)
                if 'unique' not in self.cstr_d.keys():
                    self.cstr_d['unique'] = []
                self.cstr_d['unique'].append(unique_cols)
            if len(fkeys) != 0:
                for fkey in fkeys:
                    tabcol = tab_name + '.' + fkey['FName'].lower()
                    col_id = self.col2id[tabcol]
                    tab_name_p = \
                        self.schema['Tables'][int(fkey['PTable'])]['TableName'].lower()
                    tabcol_p = tab_name_p + '.' + fkey['PName'].lower()
                    col_id_p = self.col2id[tabcol_p]
                    if col_id not in self.cstr_d.keys():
                        self.cstr_d[col_id] = []
                    self.cstr_d[col_id].append({
                        "parent_col_id": col_id_p,
                        "operator": "subset",
                        "info": {}
                    })
        return self.cstr_d

    
    def parse_tab_col(self, var: str, delimiter: str="."):
        l_del, r_del = delimiter[0], delimiter[-1]
        l_pos, r_pos = var.find(l_del), var.find(r_del)+1
        table = var[0:l_pos].lower()
        col = var[r_pos:].lower()
        return table, col

    
    def parse_leftvar(self, statement: str, delimiter: str=".", operator: str=":"):
        l_opt= operator[0]
        l_optpos= statement.find(l_opt)
        l_var = statement[0:l_optpos].strip()
        l_tab, l_col = self.parse_tab_col(l_var, delimiter=delimiter)
        return l_tab, l_col
    

    def parse_tab_col(self, var: str, delimiter: str="."):
        l_del, r_del = delimiter[0], delimiter[-1]
        l_pos, r_pos = var.find(l_del), var.find(r_del)+1
        table = var[0:l_pos].lower()
        col = var[r_pos:].lower()
        return table, col


    def parse_var(self, statement: str, delimiter: str=".", operator: str="="):
        l_opt, r_opt = operator[0], operator[-1]
        l_optpos, r_optpos = statement.find(l_opt), statement.find(r_opt)+1
        l_var = statement[0:l_optpos].strip()
        r_var = statement[r_optpos:].strip()
        l_tab, l_col = self.parse_tab_col(l_var, delimiter=delimiter)
        r_tab, r_col = self.parse_tab_col(r_var, delimiter=delimiter)
        return l_tab, l_col, r_tab, r_col


    def parse_coldep(self, col_dep: str):
        l_angle = col_dep.find("<")
        r_angle = col_dep.find(">")
        equal = col_dep.find("=")
        nequal = col_dep.find("!")
        hat = col_dep.find("^")
        imply = col_dep.find("=>")
        if imply != -1:
            # => relation
            l_colval = col_dep[0:imply].strip()
            r_colval = col_dep[imply+2+1:].strip()
            l_colval, l_col_id = self.parse_colval(l_colval, update=False)
            r_colval, r_col_id = self.parse_colval(r_colval, update=False)
            if r_col_id not in self.cstr_d.keys():
                self.cstr_d[r_col_id] = []
            self.cstr_d[r_col_id].append({
                "parent_col_id": l_col_id,
                "operator": "=>",
                "info": {
                    "if": l_colval,
                    "then": r_colval
                }
            })
        elif l_angle != -1:
            if col_dep[l_angle+1] == "-":
                # <- relation
                l_tab, l_col, r_tab, r_col \
                    = self.parse_var(col_dep, operator="<-")
                l_col_id = self.col2id[l_tab+"."+l_col]
                if l_col_id not in self.cstr_d.keys():
                    self.cstr_d[l_col_id] = []
                self.cstr_d[l_col_id].append({
                    "parent_col_id": self.col2id[r_tab+'.'+r_col],
                    "operator": "subset",
                    "info": {}
                })
            else:
                # < relation
                l_tab, l_col, r_tab, r_col \
                    = self.parse_var(col_dep, operator="<")
                l_col_id = self.col2id[l_tab+"."+l_col]
                r_col_id = self.col2id[r_tab+"."+r_col]
                if l_col_id not in self.cstr_d.keys():
                    self.cstr_d[l_col_id] = []
                self.cstr_d[l_col_id].append({
                    "parent_col_id": r_col_id,
                    "operator": "<",
                    "info": {}
                })
                if r_col_id not in self.cstr_d.keys():
                    self.cstr_d[r_col_id] = []
                self.cstr_d[r_col_id].append({
                    "parent_col_id": l_col_id,
                    "operator": ">",
                    "info": {}
                })
        elif r_angle != -1:
            # > relation
            l_tab, l_col, r_tab, r_col \
                = self.parse_var(col_dep, operator=">")
            l_col_id = self.col2id[l_tab+"."+l_col]
            r_col_id = self.col2id[r_tab+"."+r_col]
            if l_col_id not in self.cstr_d.keys():
                    self.cstr_d[l_col_id] = []
            self.cstr_d[l_col_id].append({
                "parent_col_id": r_col_id,
                "operator": ">",
                "info": {}
            })
            if r_col_id not in self.cstr_d.keys():
                    self.cstr_d[r_col_id] = []
            self.cstr_d[r_col_id].append({
                "parent_col_id": l_col_id,
                "operator": "<",
                "info": {}
            })
        elif nequal != -1:
            # != relation
            l_tab, l_col, r_tab, r_col \
                = self.parse_var(col_dep, operator="!=")
            l_col_id = self.col2id[l_tab+"."+l_col]
            r_col_id = self.col2id[r_tab+"."+r_col]
            if l_col_id not in self.cstr_d.keys():
                    self.cstr_d[l_col_id] = []
            self.cstr_d[l_col_id].append({
                "parent_col_id": r_col_id,
                "operator": "!=",
                "info": {}
            })
            if r_col_id not in self.cstr_d.keys():
                    self.cstr_d[r_col_id] = []
            self.cstr_d[r_col_id].append({
                "parent_col_id": l_col_id,
                "operator": "!=",
                "info": {}
            })
        elif equal != -1:
            # = relation
            l_tab, l_col, r_tab, r_col \
                = self.parse_var(col_dep, operator="=")
            l_col_id = self.col2id[l_tab+"."+l_col]
            r_col_id = self.col2id[r_tab+"."+r_col]
            if l_col_id not in self.cstr_d.keys():
                    self.cstr_d[l_col_id] = []
            self.cstr_d[l_col_id].append({
                "parent_col_id": r_col_id,
                "operator": "=",
                "info": {}
            })
            if r_col_id not in self.cstr_d.keys():
                    self.cstr_d[r_col_id] = []
            self.cstr_d[r_col_id].append({
                "parent_col_id": l_col_id,
                "operator": "=",
                "info": {}
            })
        elif hat != -1:
            # ^ relation
            l_tab, l_col, r_tab, r_col \
                = self.parse_var(col_dep, operator="^")
            l_col_id = self.col2id[l_tab+"."+l_col]
            r_col_id = self.col2id[r_tab+"."+r_col]
            if "join" not in self.cstr_d.keys():
                    self.cstr_d["join"] = []
            self.cstr_d["join"].append([
                    l_col_id,
                    r_col_id
            ])


    def parse_colval(self, col_val: str, update: bool=True):
        if "(s)" in col_val:
            is_soft = True
            pos = col_val.find("(s)")
            col_val = col_val[pos+3:]
        else:
            is_soft = False
        colon = col_val.find("|")
        l_angle = col_val.find("<")
        r_angle = col_val.find(">")
        nequal = col_val.find("!")
        equal = col_val.find("=")
        cstr = {}
        l_col_id = -1
        if colon != -1:
            # | relation
            l_tab, l_col = self.parse_leftvar(col_val, operator="|")
            l_col_id = self.col2id[l_tab+"."+l_col]
            l_var = col_val[colon+1:].strip().split("+")
            var = [v.strip() for v in l_var]
            cstr = {
                "parent_col_id": -1,
                "operator": "|",
                "is_soft": is_soft,
                "info": {
                    "types": var
                }
            }
            if update:
                if l_col_id not in self.cstr_d.keys():
                        self.cstr_d[l_col_id] = []
                self.cstr_d[l_col_id].append(cstr)
        if l_angle != -1:
            if col_val[l_angle+1] == "-":
                # <- relation
                l_tab, l_col = self.parse_leftvar(col_val, operator="<-")
                l_col_id = self.col2id[l_tab + '.' + l_col]
                if "[" in col_val:
                    l_sqr = col_val.find("[") + 1
                    r_sqr = col_val.find("]")
                    ran = col_val[l_sqr: r_sqr].split(',')
                    cstr = {
                        "parent_col_id": -1,
                        "operator": "range",
                        "is_soft": is_soft,
                        "info": {
                            "discrete": False,
                            "lower": ran[0].strip(),
                            "upper": ran[1].strip()
                        }
                    }
                    if update:
                        if l_col_id not in self.cstr_d.keys():
                            self.cstr_d[l_col_id] = []
                        self.cstr_d[l_col_id].append(cstr)
                elif "{" in col_val:
                    l_cur = col_val.find("{")+1
                    r_cur = col_val.find("}")
                    var = col_val[l_cur, r_cur].split(",")
                    var = [v.strip() for v in var]
                    cstr = {
                        "parent_col_id": -1,
                        "operator": "range",
                        "is_soft": is_soft,
                        "info": {
                            "discrete": True,
                            "set": var
                        }
                    }
                    if update:
                        if l_col_id not in self.cstr_d.keys():
                            self.cstr_d[l_col_id] = []
                        self.cstr_d[l_col_id].append(cstr)
            else:
                # < relation
                l_tab, l_col = self.parse_leftvar(col_val, operator="<")
                l_col_id = self.col2id[l_tab + '.' + l_col]
                r_val = col_val[l_angle+1:].strip()
                cstr = {
                    "parent_col_id": -1,
                    "operator": "range",
                    "is_soft": is_soft,
                    "info": {
                        "discrete": False,
                        "lower": None,
                        "upper": r_val
                    }
                }
                if update:
                    if l_col_id not in self.cstr_d.keys():
                        self.cstr_d[l_col_id] = []
                    self.cstr_d[l_col_id].append(cstr)
        if r_angle != -1:
            if col_val[r_angle-1] == '-':
                # -> relation
                l_tab, l_col = self.parse_leftvar(col_val, operator="->")
                l_col_id = self.col2id[l_tab + '.' + l_col]
                l_cur = col_val.find("{")+1
                r_cur = col_val.find("}")
                var = col_val[l_cur:r_cur].split(",")
                var = [v.strip() for v in var]
                if var[0].isnumeric():
                    var = [int(v) for v in var]
                cstr = {
                    "parent_col_id": -1,
                    "operator": "superset",
                    "is_soft": is_soft,
                    "info": {
                        "set": var
                    }
                }
                if update:
                    if l_col_id not in self.cstr_d.keys():
                            self.cstr_d[l_col_id] = []
                    self.cstr_d[l_col_id].append(cstr)
            else:
                # > relation
                l_tab, l_col = self.parse_leftvar(col_val, operator=">")
                l_col_id = self.col2id[l_tab + '.' + l_col]
                r_val = col_val[r_angle+1:].strip()
                cstr = {
                    "parent_col_id": -1,
                    "operator": "range",
                    "is_soft": is_soft,
                    "info": {
                        "discrete": False,
                        "lower": r_val,
                        "upper": None
                    }
                }
                if update:
                    if l_col_id not in self.cstr_d.keys():
                        self.cstr_d[l_col_id] = []
                    self.cstr_d[l_col_id].append(cstr)
        if equal != -1:
            if col_val[equal-1] == "!":
                # != relation
                l_tab, l_col = self.parse_leftvar(col_val, operator="!=")
                l_col_id = self.col2id[l_tab + '.' + l_col]
                r_val = col_val[nequal+2:].strip()
                cstr = {
                    "parent_col_id": -1,
                    "operator": "!=",
                    "is_soft": is_soft,
                    "info": {
                        "value": [r_val]
                    }
                }
                if update:
                    if l_col_id not in self.cstr_d.keys():
                        self.cstr_d[l_col_id] = []
                    self.cstr_d[l_col_id].append(cstr)
            else:
                # = relation
                l_tab, l_col = self.parse_leftvar(col_val, operator="=")
                l_col_id = self.col2id[l_tab + '.' + l_col]
                r_val = col_val[equal+1:].strip()
                cstr = {
                    "parent_col_id": -1,
                    "operator": "=",
                    "is_soft": is_soft,
                    "info": {
                        "value": [r_val]
                    }
                }
                if update:
                    if l_col_id not in self.cstr_d.keys():
                        self.cstr_d[l_col_id] = []
                    self.cstr_d[l_col_id].append(cstr)
        return cstr, l_col_id

    def parse_rowdep(self, row_dep: str):
        l_para = row_dep.find("(")
        r_para = row_dep.find(")") +1
        func = row_dep[0:l_para].strip()
        if func in ["inc", "consec", "bound"]:
            # single argument
            tab, col = self.parse_tab_col(row_dep[l_para+1:r_para-1].strip())
            col_id = self.col2id[tab + '.' + col]

            if col_id not in self.cstr_d:
                    self.cstr_d[col_id] = []
            self.cstr_d[col_id].append({
                "parent_col_id": -1,
                "operator": func,
                "info": {}
            })
        elif func == "unique":
            # multiple arguments
            var = row_dep[l_para+1: r_para-1].strip().split(",")
            var = [v.strip() for v in var]
            var = [self.parse_tab_col(v) for v in var]
            var = [self.col2id[v[0] + '.' + v[1]] for v in var]
            if "unique" not in self.cstr_d.keys():
                self.cstr_d["unique"] = []
            self.cstr_d["unique"].append(var)

    
    def parse_constraints(self):
        self.gen_schema_constraints()
        if self.cstr_d == None:
            self.cstr_d = {}
        if "col_dep" in self.cstr_raw_dict.keys():
            coldeps = self.cstr_raw_dict["col_dep"].split(";")
            for coldep in coldeps:
                self.parse_coldep(coldep.strip())
        if "col_val" in self.cstr_raw_dict.keys():
            colvals = self.cstr_raw_dict["col_val"].split(";")
            for colval in colvals:
                self.parse_colval(colval.strip())
        if "row_dep" in self.cstr_raw_dict.keys():
            rowdeps = self.cstr_raw_dict["row_dep"].split(";")
            for rowdep in rowdeps:
                self.parse_rowdep(rowdep.strip())
        return self.cstr_d