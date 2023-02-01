from ast import Add
import itertools
import logging
from faker import Faker
from collections import defaultdict

from testing.util import topo_sort_cols
from testing.testing_parser import Parser
import random
import math
import datetime
from copy import deepcopy

class Constrained_Random_Table_Generation():
    def __init__(self, schema: dict, sizes, fake=None, raw_cstr: dict={}, seed=2333, is_extreme: bool=False) -> None:
        # init random seeds
        random.seed(seed)
        if fake:
            self.fake = fake
        else:
            self.fake = Faker()
        Faker.seed(seed)

        # init parser
        self.parser = Parser(schema, raw_cstr)
        self.id2col, self.col2id = self.parser.gen_col_dicts(sizes)
        self.cstr_d = self.parser.parse_constraints()
        for i in range(len(self.id2col)):
            if i not in self.cstr_d.keys():
                self.cstr_d[i] = []
        self.cstr_d['join'] = []

        # init tables
        self.tables = []
        self.columns = [[] for _ in range(len(self.col2id))]
        self.col_order = topo_sort_cols(len(self.col2id), self.cstr_d)

        # construct table translation
        self.tab2id = {}
        self.id2tab = {}
        tableId = 0
        for table in schema['Tables']:
            self.tab2id[table['TableName'].lower()] = tableId
            self.id2tab[tableId] = table['TableName'].lower()
            self.tables.append(defaultdict(list))
            tableId += 1
        
        # construct col-tab translation
        self.colId2tabId = {}
        for col_id in self.id2col.keys():
            self.colId2tabId[col_id] \
                = self.tab2id[self.id2col[col_id]['table_name']]

        # init generators
        self.generators = {}
        
        # other heuristics
        self.is_extreme = is_extreme


    def generate(self):
        # generate col by col
        for col_id in self.col_order:
            if self.columns[col_id] == []:
                # not yet generated
                if col_id in self.cstr_d.keys():
                    to_remove = []
                    parent_cols = {}
                    for i, col_cstr in enumerate(self.cstr_d[col_id]):
                        parent_col_id = col_cstr['parent_col_id']
                        if parent_col_id != -1:
                            parent_cols[parent_col_id] = self.columns[parent_col_id]
                            if self.columns[parent_col_id] == []:
                                if col_cstr['operator'] in ['<', '>', '=', '!=']:
                                    to_remove.append(i)
                                else:
                                    # print(f"Fail to generate due to bad dependency.")
                                    logging.info(f"Fail to generate due to bad dependency.")
                                    raise Exception
                    to_remove.sort()
                    count_rm = 0
                    for trm in to_remove:
                        self.cstr_d[col_id].pop(trm-count_rm)
                        count_rm += 1
                    # print(f"generating col {self.id2col[col_id]} with constraints\
                    #     {self.cstr_d[col_id]}")
                    self.generate_col(col_id, self.cstr_d[col_id])
                else:
                    # print(f"generating col {self.id2col[col_id]}")
                    self.generate_col(col_id)
            else:
                # logging.info("Warning: column {col_id} already generated")
                pass

        # enforce duplicate if no unique constraint
        need_dup = []
        if 'unique' not in self.cstr_d.keys():
            need_dup = list(range(len(self.columns)))
        else:
            # get unique col
            unique_cols = []
            for col_id in range(len(self.columns)):
                is_unique = False
                for unique_pair in self.cstr_d['unique']:
                    if col_id in unique_pair:
                        unique_cols.append(col_id)
            # get unique tab
            unique_tabs = []
            for ucol_id in unique_cols:
                if self.colId2tabId[ucol_id] not in unique_tabs:
                    unique_tabs.append(self.colId2tabId[ucol_id])
            # get need-dup col
            for col_id in range(len(self.columns)):
                if self.colId2tabId[col_id] not in unique_tabs:
                    need_dup.append(col_id)
        # debug output
        # print(self.cstr_d)
        # if len(need_dup) != 0:
        #     print(f'need dup: {need_dup}')
        for col_id in need_dup:
            self.columns[col_id] += self.columns[col_id][len(self.columns[col_id])//2:]        

        # reformat columns to tables
        for col_id, col in enumerate(self.columns):
            col_name = self.id2col[col_id]['column_name']
            table_id = self.colId2tabId[col_id]
            self.tables[table_id][col_name] = col
        # reformat columns to compatible databases
        databases = {}
        databases_schema = {}
        for table_id, tab in enumerate(self.tables):
            tab_name = self.id2tab[table_id]
            cols = []
            col_names = []
            for col_name in tab.keys():
                cols.append(tab[col_name])
                col_names.append(col_name)
            cols = list(zip(*cols))
            databases[tab_name] = cols
            databases_schema[tab_name] = col_names

        return databases, databases_schema

    def row_dep_generator(self, _type, size, operator):
        if operator == 'inc':
            # only support int for inc operator
            if _type == 'int':
                return list(range(1,size+1))
            else:
                logging.info(f"not support type {_type} for inc operator")
                raise Exception
        elif operator == 'consec':
            # only support int for consec operator
            if _type == 'int':
                output = [0]
                for _ in range(size-1):
                    decision = random.uniform(0,1)
                    if decision > 0.5:
                        output.append(output[-1] + 1)
                    else:
                        output.append(output[-1])
                return output
            else:
                logging.info(f"not support type {_type} for consec operator")
                raise Exception
        elif operator == 'bound':
            if _type == 'int':
                return random.sample(list(range(1, size+1)), k=size)
            else:
                # print(f"not support type {_type} for consec operator")
                logging.info(f"not support type {_type} for consec operator")
                raise Exception
        else:
            # print(f"not support row dep {operator}")
            logging.info(f"not support row dep {operator}")
            raise Exception

    def check_value(self, _type, operator, value, reference):
        # convert type if string
        if isinstance(value, str):
            value = self.typing_string(_type, value)
        if isinstance(reference, str):
            reference = self.typing_string(_type, reference)
        if operator == '>':
            if value >= reference:
                return True
            else:
                return False
        elif operator == '<':
            if value <= reference:
                return True
            else:
                return False
        elif operator == '!=':
            if value != reference:
                return True
            else:
                return False
        elif operator == '=':
            if value == reference:
                return True
            else:
                logging.info(f"Not support operator {operator}")
                return False
        else:
            raise ValueError

    def check_col_val_cstr(self, _type, cstr, val):
        if cstr['operator'] == '=':
            ref = cstr['info']['value']
            val = str(val)
            if val in ref:
                return True
            else:
                return False
        elif cstr['operator'] == '!=':
            val = str(val)
            ref = cstr['info']['value']
            if val not in ref:
                return True
            else:
                return False
        if isinstance(val, str):
            val = self.typing_string(_type, val)
        if cstr['operator'] == 'range':
            lower, upper = cstr['info']['lower'], cstr['info']['upper']
            lower, upper = self.get_bound(_type, lower, upper)
            if val >= lower and val <= upper:
                return True
            else:
                return False
        else:
            logging.info(f"not support {cstr} in checking col val constraints")
    
    def unique_gen(self, types, sizes, cols_cstr: list=[], join: list=[[],[]], unique_guide: str=None):
        # types and sizes must have the same length
        assert(len(types) == len(sizes))
        # so is cstr
        assert(len(types) == len(cols_cstr))
        # no more that two cols
        assert(len(types) <= 2)
        if len(types) == 2:
            # sizes must be consistent
            assert(sizes[1] == sizes[0])
        # calculate random generation size
        if len(types) == 2 and len(cols_cstr) > 0 and "operator" in cols_cstr[0] and cols_cstr[0]["operator"] == '=' and cols_cstr[0]['is_soft'] and types[0] == types[1]:
            total_size = sizes[0] + sizes[1]
            unique_pool = self.unique_gen_helper(types[0], total_size)
            output1 = unique_pool[:sizes[0]*2//3]
            output2 = output1[len(output1)//2:]
            output2 += output1[:len(output1)//2]
            output1 += unique_pool[:(sizes[0] - sizes[0]*2//3)]
            output2 += unique_pool[1:(1+sizes[1]-len(output2))]
            print(len(output1), len(output2))
            outputs = [output1, output2]
            return outputs
        gen_sizes = []
        max_lens = [self.get_max_len(t, c) for t, c in zip(types, cols_cstr)]
        if math.prod(max_lens) < sizes[0]:
            sizes[0] = math.prod(max_lens)
        max_prod = math.prod(max_lens)
        prod = min(random.choice(range(sizes[0], max_prod+1)), 2*sizes[0])
        if len(types) == 1:
            gen_sizes.append(prod)
        else:
            # two cols, two sizes
            size1 = min(int(math.sqrt(prod)), max_lens[0])
            size2 = math.ceil(prod / size1)
            # fail to resolve in the first way
            if size2 > max_lens[1]:
                size2 = max_lens[1]
                size1 = math.ceil(prod / size2)
            gen_sizes = [size1, size2]
        if unique_guide:
            guides = unique_guide.split('-')
            if guides[0] != '*':
                gen_sizes = [int(guides[0]), math.ceil(prod / int(guides[0]))]
            else:
                gen_sizes = [math.ceil(prod / int(guides[1])), int(guides[1])]

        # generate col by col
        output = []
        for i in range(len(types)):
            if len(cols_cstr[i]) == 0:
                output.append(self.unique_gen_helper(types[i], gen_sizes[i], join=join[i]))
            else:
                # print('here')
                # support range, subset, superset, exclusion, inc, consec
                if cols_cstr[i]['operator'] == 'range':
                    if cols_cstr[i]['info']['discrete']:
                        output.append(self.unique_gen_helper(
                            types[i], gen_sizes[i], 
                            subset = cols_cstr[i]['info']['set']
                        ))
                    else:
                        output.append(self.unique_gen_helper(
                            types[i], gen_sizes[i],
                            _range=[
                                cols_cstr[i]['info']['lower'],
                                cols_cstr[i]['info']['upper']
                            ]
                        ))
                elif cols_cstr[i]['operator'] == 'subset':
                    parent = list(set(self.columns[cols_cstr[i]['parent_col_id']]))
                    output.append(self.unique_gen_helper(
                        types[i], gen_sizes[i],
                        subset=parent
                    ))
                elif cols_cstr[i]['operator'] == 'superset':
                    output.append(self.unique_gen_helper(
                        types[i], gen_sizes[i],
                        superset=cols_cstr[i]['info']['set']
                    ))
                elif cols_cstr[i]['operator'] == '!=':
                    output.append(self.unique_gen_helper(
                        types[i], gen_sizes[i],
                        exclusion=cols_cstr[i]['info']['value']
                    ))
                elif cols_cstr[i]['operator'] == '=':
                    if cols_cstr[i]['parent_col_id'] != -1:
                        # print(f'not support col = in unique gen')
                        output.append(self.unique_gen_helper(
                            types[i], gen_sizes[i]
                        ))
                    else:
                        output.append(self.unique_gen_helper(
                            types[i], gen_sizes[i],
                            superset=cols_cstr[i]['info']['value']
                        ))
                elif cols_cstr[i]['operator'] == 'inc':
                    output.append(self.unique_gen_helper(
                        types[i], gen_sizes[i],
                        is_inc=True
                    ))
                elif cols_cstr[i]['operator'] == 'consec':
                    output.append(self.unique_gen_helper(
                        types[i], gen_sizes[i],
                        is_consec=True
                    ))
                elif len(join) != 0:
                    output.append(self.unique_gen_helper(
                        types[i], gen_sizes[i],
                        join=join[i]
                    ))
                else:
                    # print(f"Not support operator {cols_cstr[i]['operator']}\
                    #     in unique generation yet.")
                    logging.info(f"Not support operator {cols_cstr[i]['operator']}\
                        in unique generation yet.")
                    raise Exception
        if len(output) == 2:
            cart_prod = list(itertools.product(output[0], output[1]))
            output = random.sample(cart_prod, k=sizes[0])
            output = list(zip(*output))
        else:
            output[0] = output[0][:sizes[0]]
        return output

    def unique_gen_helper(self, _type, size, _range: list=[], is_inc: bool=False, \
        is_consec: bool=False, superset: list=[], subset: list=[], exclusion: list=[], join: list=[], unique_guide: str=None):
        output = []
        if len(join) != 0:
            lower, upper = self.get_bound(_type)
            generator = self.type2values(_type, lower, upper, unique=True)
            output += random.sample(join, k=size//2)
            for i in range(size//2):
                value  = output[i]
                while output.count(value) > 1:
                    value = generator(1)[0]
                    output[i] = value
            for i in range(size-size//2):
                value = generator(1)[0]
                while value in output:
                    value = generator(1)[0]
                output.append(value)
            return output
        # special cases: is_inc, is_consec, subset
        if is_inc:
            return self.row_dep_generator(_type, size, 'inc')
        elif is_consec:
            return self.row_dep_generator(_type, size, 'consec')
        elif len(subset) != 0:
            assert(len(subset) >= size)
            return random.sample(subset, k=size)
        # general cases: range
        lower, upper = self.get_bound(_type, _range, is_extreme=False)
        # generate
        gen_functor = self.type2values(_type, lower, upper, True)
        output = gen_functor(size)
        # post generation check
        if len(superset) != 0 or len(exclusion) != 0:
            value_need = superset + exclusion
            # print(value_need)
            for i, spst in enumerate(value_need):
                if spst not in output:
                    output[i] = self.typing_string(_type, spst)
            random.shuffle(output)
        self.fake.unique.clear()
        return output
                        
    def typing_string(self, _type: str, string: str):
        if _type == 'int':
            return int(string)
        elif _type == 'date':
            string = string.split(' ')[0]
            return datetime.date.fromisoformat(string)
        elif _type == 'time':
            return datetime.datetime.strptime(string, "%Y-%m-%d-%H-%M-%S")
        elif _type == 'numeric' or _type == 'decimal':
            return float(string)
        return string

    def get_bound(self, _type, _range:list=[], is_extreme: bool=False):
        lower, upper = None, None
        if not is_extreme:
            if _type == 'int':
                lower = 0
                upper = 2147483647
            elif _type == 'date':
                lower = datetime.date(1,1,1)
                upper = datetime.date(9999,12,30)
            elif _type == 'time':
                lower = datetime.datetime(1,1,1,0,0,0)
                upper = datetime.datetime(9999,12,31,23,59,59)
            elif _type == 'numeric' or _type == 'decimal':
                lower = 0.0
                upper = 2147483647.0
            if len(_range) != 0:
                lower = self.typing_string(_type, _range[0]) if _range[0] is not None else lower
                upper = self.typing_string(_type, _range[1]) if _range[1] is not None else upper
        else:
            if len(_range) != 0:
                lower = self.typing_string(_type, _range[0]) if _range[0] is not None else lower
                upper = self.typing_string(_type, _range[1]) if _range[1] is not None else upper
            def add_diff(diff, lower, upper):
                if lower != None:
                    upper = lower + diff
                elif upper != None:
                    lower = upper - diff
                return lower, upper
            if _type == 'int':
                lower, upper = add_diff(3, lower, upper)
                if lower == None or upper == None:
                    lower = 97
                    upper = 100
            elif _type == 'date':
                lower, upper = add_diff(datetime.timedelta(days=3), lower, upper)
                if lower == None or upper == None:
                    lower = datetime.date(2020,1,1)
                    upper = datetime.date(2020,1,3)
            elif _type == 'time':
                lower, upper = add_diff(datetime.timedelta(seconds=3), lower, upper)
                if lower == None or upper == None:
                    lower = datetime.datetime(2020,1,1,0,0,0)
                    upper = datetime.datetime(2020,1,1,0,0,3)
            elif _type == 'numeric' or _type == 'decimal':
                lower, upper = add_diff(1, lower, upper)
                if lower == None or upper == None:
                    lower = 0.0
                    upper = 1.0
        return lower, upper
    
    def type2values(self, _type, lower, upper, unique):
        # add enforce duplicate for varchar
        if _type == 'int':
            if unique:
                return lambda k: random.sample(range(lower, upper+1), k)
            else:
                # print(lower, upper)
                return lambda k: random.choices(range(lower, upper+1), k=k)
        elif _type == 'date':
            if unique:
                return lambda k: [self.fake.unique.date_between(lower, upper) for _ in range(k)]
            else:
                return lambda k: [self.fake.date_between(lower, upper) for _ in range(k)]
        elif _type == 'time':
            if unique:
                return lambda k: [self.fake.unique.date_time_between(lower, upper) for _ in range(k)]
            else:
                return lambda k: [self.fake.date_time_between(lower, upper) for _ in range(k)]
        elif _type == 'varchar':
            if unique:
                return lambda k: [self.fake.unique.name() for _ in range(k)]
            else:
                def generator(size):
                    output = [self.fake.unique.name() for _ in range(size*2//3)]
                    output += output[:(size-size*2//3)]
                    return output
                return generator
        elif _type == 'numeric' or _type == 'decimal':
            assert(not unique)
            return lambda k: [random.uniform(lower, upper) for _ in range(k)]
        elif _type[0:4] == 'enum':
            enums = _type.split(',')[1:]
            if unique:
                return lambda k: random.sample(enums, k)
            else:
                return lambda k: random.choices(enums, k=k)
        elif _type == 'null':
            return lambda k: ['null'] * k
        elif _type == 'bool':
            return lambda k: random.choices([True, False], k=k)
        else:
            # print(f"type {_type} not support yet")
            logging.info(f"type {_type} not support yet")
            raise Exception

    def get_col_generator(self, _type: list=[], _range: list=[], subset: list=[], soft: list=[], join: list=[]):
        if _range != [] and subset != [] and len(_type) == 1:
            _range = _range[0]
            subset = subset[0]
            def generator(k):
                output = []
                lower, upper = self.get_bound(_type[0], _range, is_extreme=self.is_extreme)
                subset_generator = lambda k: random.choices(subset, k=k)
                for _ in range(k):
                    val = subset_generator(1)[0]
                    while val < lower or val > upper:
                        val = subset_generator
                    output.append(val)
                return output
            return generator
        elif subset != [] and len(_type) == 1:
            subset = subset[0]
            return lambda k: random.choices(subset, k=k)
        elif _range != [] and len(_type) == 1:
            _range = _range[0]
            lower, upper = self.get_bound(_type[0], _range, is_extreme=self.is_extreme)
            return self.type2values(_type[0], lower, upper, unique=False)
        elif len(_type) == 2 and _range == [] and subset == []:
            def gen_type_override(k):
                output = []
                lower1, upper1 = self.get_bound(_type[0], [None, None], is_extreme=self.is_extreme)
                lower2, upper2 = self.get_bound(_type[1], [None, None], is_extreme=self.is_extreme)
                generator1 = self.type2values(_type[0], lower1, upper1, unique=False)
                generator2 = self.type2values(_type[1], lower2, upper2, unique=False)
                for _ in range(k):
                    if random.uniform(0,1) < 0.5:
                        output += generator1(1)
                    else:
                        output += generator2(1)
                return output
            return gen_type_override
        elif len(_type) == 2 and subset == []:
            def generator(k):
                output = []
                lower1, upper1 = self.get_bound(_type[0], _range[0], is_extreme=self.is_extreme)
                lower2, upper2 = self.get_bound(_type[1], _range[1], is_extreme=self.is_extreme)
                generator1 = self.type2values(_type[0], lower1, upper1, unique=False)
                generator2 = self.type2values(_type[1], lower2, upper2, unique=False)
                for _ in range(k):
                    if random.uniform(0,1) < 0.5:
                        output += generator1(1)
                    else:
                        output += generator2(1)
                return output
            return generator
        elif len(_type) == 2 and _range == [] and subset != []:
            def generator(k):
                output = []
                if len(subset) == 1:
                    generator1 = lambda k: random.choices(subset[0], k=k)
                    generator2 = self.type2values(_type[1], None, None, unique=False)
                else:
                    generator1 = lambda k: random.choices(subset[0], k=k)
                    generator2 = lambda k: random.choices(subset[1], k=k)
                for _ in range(k):
                    if random.uniform(0,1) < 0.5:
                        output += generator1(1)
                    else:
                        output += generator2(1)
                return output
            return generator
        elif len(soft) != 0:
            if soft[0] == 'range':
                lower1, upper1 = self.get_bound(_type[0], soft[1:], is_extreme=self.is_extreme)
                lower2, upper2 = self.get_bound(_type[0], is_extreme=self.is_extreme)
                generator1 = self.type2values(_type[0], lower1, upper1, unique=False)
                generator2 = self.type2values(_type[1], lower2, upper2, unique=False)
                def generator(k):
                    output = []
                    for _ in range(k):
                        if random.uniform(0,1) < 0.5:
                            output += generator1(1)
                        else:
                            output += generator2(1)
                    return output
                return generator
            elif soft[0] in ['=', '!=']:
                if len(soft[1]) > 10:
                    values = []
                    for value in soft[1]:
                        values.append(self.typing_string(_type[0], value))
                    lower, upper = self.get_bound(_type[0], is_extreme=self.is_extreme)
                    generator = self.type2values(_type[0], lower, upper, unique=False)
                    def values_generator(k):
                        output = []
                        for i in range(k):
                            d = random.uniform(0,1)
                            if d < 0.5:
                                output += generator(1)
                            else:
                                output += [values[i]]
                        return output
                    return values_generator
                else:
                    values = []
                    for value in soft[1]:
                        values.append(self.typing_string(_type[0], value))
                    lower, upper = self.get_bound(_type[0], is_extreme=self.is_extreme)
                    generator = self.type2values(_type[0], lower, upper, unique=False)
                    def value_generator(k):
                        output = []
                        for _ in range(k):
                            d = random.uniform(0,1)
                            if d < 0.5:
                                output += generator(1)
                            else:
                                output += [random.choice(values)]
                        return output
                    return value_generator
        elif len(join) != 0:
            col_id = join[0]
            subset = self.columns[col_id]
            generator1 = lambda k: random.choices(subset, k=k)
            lower, upper = self.get_bound(_type[0], is_extreme=self.is_extreme)
            generator2 = self.type2values(_type[0], lower, upper, False)
            def generator(k):
                output = []
                for _ in range(k):
                    d = random.uniform(0,1)
                    if d < 0.5:
                        output += generator1(1)
                    else:
                        output += generator2(1)
                return output
            return generator
        else:
            lower, upper = self.get_bound(_type[0], is_extreme=self.is_extreme)
            return self.type2values(_type[0], lower, upper, unique=False)
            

    def get_max_len(self, _type:str, cstr:dict={}):
        max_len = 0
        if _type == 'int':
            max_len = 2147483647
        elif _type == 'date':
            max_len = 3649635
        elif _type == 'bool':
            max_len = 2
        elif _type[:4] == 'enum':
            max_len = _type.count(',')
        else:
            max_len = 2147483647
        if len(cstr) != 0:
            # only support estimate max len for subset or range
            # cstr = cstr[0]
            if cstr['operator'] == 'range':
                if cstr['info']['discrete']:
                    max_len = len(cstr['info']['set'])
                else:
                    if _type == 'int':
                        upper = int(cstr['info']['upper']) if cstr['info']['upper'] is not None else 2147483647
                        lower = int(cstr['info']['lower']) if cstr['info']['lower'] is not None else 0
                        max_len = upper - lower + 1
                    elif _type == 'date':
                        upper = cstr['info']['upper'].split(' ')[0] if cstr['info']['upper'] is not None else '9999-12-31'
                        lower = cstr['info']['lower'].split(' ')[0] if cstr['info']['lower'] is not None else '1-1-1'
                        upper_date = datetime.datetime.strptime(upper, "%Y-%m-%d")
                        lower_date = datetime.datetime.strptime(lower, "%Y-%m-%d")
                        diff = upper_date.date() - lower_date.date()
                        max_len = int(diff.days) + 1
            elif cstr['operator'] == 'subset':
                parent = list(set(self.columns[cstr['parent_col_id']]))
                max_len = len(parent)
        return max_len

    def generate_col(self, col_id, col_cstr: list=[]):
        _type = [self.id2col[col_id]['type']]
        size = self.id2col[col_id]['size']
        if col_cstr == []:
            is_unique = False
            if 'unique' in self.cstr_d.keys():
                for unique_cols in self.cstr_d['unique']:
                    if col_id in unique_cols:
                        unique_guide = None
                        if isinstance(unique_cols[-1], str) and '-' in unique_cols[-1]:
                            c_col_id, num = unique_cols[-1].split('-')
                            if c_col_id == unique_cols[0]:
                                unique_guide = f"{num}-*"
                            else:
                                unique_guide = f"*-{num}"
                            unique_cols = unique_cols[:-1]
                        types = [self.id2col[i]['type'] for i in unique_cols]
                        sizes = [self.id2col[i]['size'] for i in unique_cols]
                        cstrs = [self.cstr_d[i] for i in unique_cols]
                        for i in range(len(cstrs)):
                            if len(cstrs[i]) > 0:
                                cstrs[i] = cstrs[i][0]
                            else:
                                cstrs[i] = {}
                        join = [[], []]
                        for join_pair in self.cstr_d['join']:
                            for i, col in enumerate(unique_cols):
                                if join_pair[0] == col:
                                    join[i] = self.columns[join_pair[1]]
                                elif join_pair[1] == col:
                                    join[i] = self.columns[join_pair[0]]
                        output = self.unique_gen(types, sizes, cstrs, join=join, unique_guide=unique_guide)
                        for i, col in enumerate(unique_cols):
                            self.columns[col] = output[i]
                        is_unique = True
            if not is_unique:
                join = []
                for join_list in self.cstr_d['join']:
                    if col_id == join_list[0]:
                        if self.columns[join_list[1]] != []:
                            join.append(join_list[1])
                            break
                    elif col_id == join_list[1]:
                        if self.columns[join_list[0]] != []:
                            join.append(join_list[0])
                            break
                generator = self.get_col_generator(_type, join=join)
                self.columns[col_id] = generator(size)
                self.generators[col_id] = generator
                return
        else:
            is_unique = False
            if 'unique' in self.cstr_d.keys():
                for unique_cols in self.cstr_d['unique']:
                    if col_id in unique_cols:
                        types = [self.id2col[i]['type'] for i in unique_cols]
                        sizes = [self.id2col[i]['size'] for i in unique_cols]
                        is_unique = True
                        break
            if is_unique:
                cols_cstr = []
                if isinstance(unique_cols[-1], str):
                    unique_cols = unique_cols[:-1]
                for col_id in unique_cols:
                    # only support no more than one constraint for unique cols
                    # assert(len(self.cstr_d[col_id]) <= 1)
                    # if len(self.cstr_d[col_id]) > 1:
                    #     print("Warning: more than one cstr to solve in unique cols")
                    if self.cstr_d[col_id] != []:
                        # support subset <-, superset ->, !=, inc, consec
                        cols_cstr.append(self.cstr_d[col_id][0])
                    else:
                        cols_cstr.append({})
                output = self.unique_gen(types, sizes, cols_cstr=cols_cstr)
                for i, col_id in enumerate(unique_cols):
                    self.columns[col_id] = output[i]
            else:
                # special cases: row dep
                _type = [self.id2col[col_id]['type']]
                size = self.id2col[col_id]['size']
                for cstr in col_cstr:
                    if cstr['operator'] in ['inc', 'consec', 'bound']:
                        if len(col_cstr) > 1:
                            logging.info(f"not support row dep with other constraints")
                            raise Exception
                        self.columns[col_id] = self.row_dep_generator(_type[0], size, cstr['operator'])
                        return
                # general cases: no row dep
                to_solve_cstr = []
                # get column generator to 
                # satisfy subset, range, and type override constraints
                types, subset, ranges, soft, join = [], [], [], [], []
                for i, cstr in enumerate(col_cstr):
                    if cstr['operator'] == 'subset':
                        subset.append(self.columns[cstr['parent_col_id']])
                    elif cstr['operator'] == 'range':
                        if cstr['is_soft']:
                            soft = ['range', cstr['info']['lower'], cstr['info']['upper']]
                        ranges.append([cstr['info']['lower'], cstr['info']['upper']])
                    elif cstr['operator'] == '|':
                        types = cstr['info']['types']
                    elif cstr['operator'] in ['=', '!=']:
                        if not cstr['is_soft']:
                            raise Exception("= constraint must be soft")
                        if cstr['parent_col_id'] == -1:
                            soft = [cstr['operator'], cstr['info']['value']]
                        elif len(self.columns[cstr['parent_col_id']]) != 0:
                            soft = [cstr['operator'], self.columns[cstr['parent_col_id']]]
                    else:
                        to_solve_cstr.append(cstr)
                # check join rel
                for join_list in self.cstr_d['join']:
                    if col_id == join_list[0]:
                        if self.columns[join_list[1]] != []:
                            join.append(join_list[1])
                            break
                    elif col_id == join_list[1]:
                        if self.columns[join_list[0]] != []:
                            join.append(join_list[0])
                            break
                _type = types if types != [] else _type
                generator = self.get_col_generator(_type, ranges, subset, soft, join)
                self.generators[col_id] = generator
                # row by row / batch generation using generator to
                # satisfy col dep (<,>,=,!=,=>)
                if len(to_solve_cstr) == 0:
                    self.columns[col_id] = generator(size)
                elif len(to_solve_cstr) == 1:
                    cstr = to_solve_cstr[0]
                    parent_col = self.columns[cstr['parent_col_id']]
                    if cstr['operator'] in ['>', '<', '=', '!=']:
                        output = []
                        for i in range(size):
                            val = generator(1)[0]
                            ref = parent_col[i]
                            while not self.check_value(_type, cstr['operator'], val, ref):
                                val = generator(1)[0]
                            output.append(val)
                        self.columns[col_id] = output

                    elif cstr['operator'] == '=>':
                        output = []
                        for i in range(size):
                            val = generator(1)[0]
                            ref = parent_col[i]
                            l_cstr = cstr['info']['if']
                            r_cstr = cstr['info']['then']
                            if self.check_col_val_cstr(_type, l_cstr, ref):
                                while not self.check_col_val_cstr(_type, r_cstr, val):
                                    val = generator(1)[0]
                            output.append(val)
                        self.columns[col_id] = output
                    elif cstr['operator'] == 'superset':
                        output = generator(size)
                        selected = random.sample(list(range(len(output))), k=len(cstr['info']['set']))
                        for s, new_val in zip(selected, cstr['info']['set']):
                            output[s] = new_val
                        self.columns[col_id] = output
                else:
                    # print("not support solve 2 constraints")
                    raise Exception
                # TODO: try to row by row repair generated columns to 
                # satisfy inclusion constraints if any


                # TODO: check all the solved constraints to
                # see if they are still satisfied