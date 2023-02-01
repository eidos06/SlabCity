import logging
from faker import Faker
from collections import defaultdict

from util import topo_sort_tables
import random
import math
import datetime


def random_generate_tables(schema, cstr, sizes, seed=2333):
    # 0. choose hyperparameters
    num_rows = sizes
    random.seed(seed)

    fake = Faker()
    Faker.seed(seed)
    tables = []

    tablename2index = {}
    for i, table_schema in enumerate(schema["Tables"]):
        tablename2index[table_schema["TableName"]] = i

    # 1. create table and fill in primary keys and foreign keys
    tables, indices = create_tables_with_keys(fake, schema, num_rows, cstr)

    # 2. fill in all other columns
    for i in indices:
        fill_other_cols(fake, tables, schema["Tables"][i], num_rows[i], tables[i], cstr, tablename2index)

    # 3. refill in many-to-many columns that are not keys
    types = []
    if "join" in cstr.keys():
        for join_cstr in cstr["join"]:
            tab = join_cstr["extra"]["tab1"]
            col = join_cstr["extra"]["col1"]
            for table_schema in schema["Tables"]:
                if table_schema["TableName"] == tab:
                    for col_schema in table_schema["Others"]:
                        if col_schema["Name"] == col:
                            types.append(col_schema["Type"])
        if len(cstr["join"]) != 0:
            refill_join_non_key(fake, tables, num_rows, cstr["join"], tablename2index, types)

    # 4. Handling no primary keys
    # DuplicateRange: ["Percentage of rows that should be repeated per table"] e.g. [0.1, 0.0, 0.2] for three tables
    # Modification: [["Percent rows to be repeated, upper bound of how many times to be repeated (per table)"]] e.g. [[0.1, 0.3],[0.0,0.0],[0.2,0.4]]
    for i, table in enumerate(tables):
        enforce_duplicate(
            schema["Tables"][i], num_rows[i], table
        )

    # 5. force uniqueness
    if "key" in cstr.keys():
        for unique_cstr in cstr["key"]:
            tabcols = unique_cstr["extra"]["cols"]
            values = []
            for tabcol in tabcols:
                tab_idx = tablename2index[tabcol[0]]
                values.append(tables[tab_idx][tabcol[1]])
            values = list(map(list, zip(*values)))
            for i, value in enumerate(values):
                count = 0
                while values.count(value) > 1:
                    for col in schema["Tables"][tablename2index[tabcols[-1][0]]]["Others"]:
                        if col["Name"] == tabcols[-1][1]:
                            _, gen_val, _ = type_functions(fake, col)
                            break
                    values[i][-1] = gen_val()
                    value = values[i]
                    count += 1
                    if count == 10000:
                        logging.info("fail to resolve unique relation")
                        raise ValueError
            values = list(map(list, zip(*values)))
            for i, tabcol in enumerate(tabcols):
                tab_idx = tablename2index[tabcol[0]]
                tables[tab_idx][tabcol[1]] = values[i]
    return tables


def create_tables_with_keys(fake, schema, num_rows, cstr: dict):
    indices = topo_sort_tables(schema["Tables"], cstr)
    tables = [None for _ in range(len(schema["Tables"]))]
    for i in indices:
        new_table = defaultdict(list)
        fill_primary_keys(
            fake, tables, schema["Tables"][i], num_rows[i], new_table, cstr
        )
        fill_foreign_keys(tables, schema["Tables"][i], num_rows[i], new_table, cstr)
        tables[i] = new_table
    return tables, indices



def fill_primary_keys(fake: Faker, tables, table_schema, num_row, output, cstr):
    """Modify output"""
    pkeys = table_schema["PKeys"]
    fkeys = table_schema["FKeys"]
    if len(pkeys) == 0:
        return
    if len(pkeys) == 1:
        tab = table_schema["TableName"]
        col = pkeys[0]["Name"]
        tabcol = tab+"."+col
        if tabcol in cstr:
            if cstr[tabcol][0]["opt"] == "inc":
                output[pkeys[0]["Name"]] = list(range(1, num_row+1))
                return
            if cstr[tabcol][0]["opt"] == "consec":
                pkey_result = []
                pkey_result.append(0)
                for i in range(1, num_row):
                    d = random.uniform(0,1)
                    if d < 0.5:
                        pkey_result.append(pkey_result[i-1]+1)
                    else:
                        pkey_result.append(pkey_result[i-1])
                output[pkey[0]["Name"]] = pkey_result
                return
    # randomly generate the expected ratio between/among pkeys
    unique_perc = random.choices(list(range(1,11)), k=len(pkeys))
    dimension = []
    choices = []  # list of list of choices for each pkey
    # generate pkeys_choices
    for i in range(len(pkeys)):
        pkey = pkeys[i]
        try:
            # if it is also foreign key, 
            # use the primary key of the table it depends on as choice
            ptable = next(
                tables[int(fkey["PTable"])]
                for fkey in fkeys
                if fkey["FName"] == pkey["Name"]
            )
            choice = ptable[pkey["Name"]]
        except:
            num_row_gen = max(int(num_row * unique_perc[i]), 1)
            if pkey["Type"] == "date":
                num_row_gen = num_row
                choice = []
                start_date = datetime.date(1,1,1)
                for i in range(num_row_gen):
                    choice.append(start_date + datetime.timedelta(days=i))
                random.shuffle(choice)
            else:    
                _, _, gen_unique = type_functions(fake, pkey)
                choice = gen_unique(num_row_gen)
        dimension.append(len(choice))
        choices.append(choice)
    # indexing the Cartesian product of chioces according to the lexicographic order
    # e.g. dimension = [4, 3], then index = 10 = 3 * 3 + 1 means [choices[0][3], choices[1][1]]
    pkeys_indices = random.sample(range(0, math.prod(dimension)), num_row)
    def cartesian_index_to_pkey(index):
        pkey = []
        for i, dim in reversed(list(enumerate(dimension))):
            pkey.append(choices[i][index % dim])
            index = index // dim
        return list(reversed(pkey))

    pkeys_result = [
        cartesian_index_to_pkey(pkeys_index) for pkeys_index in pkeys_indices
    ]
    # transpose
    pkeys_result = list(map(list, zip(*pkeys_result)))
    # fill pkeys
    for i, pkey in enumerate(pkeys):
        # commit
        tabcol = table_schema["TableName"] + "." + pkey["Name"]
        if tabcol in cstr:
            c = cstr[tabcol][0]
            if c["opt"] == "->":
                indices = random.sample(list(range(num_row)), k=len(c["extra"]["set"]))
                for j, idx in enumerate(indices):
                    pkeys_result[i][idx] = c["extra"]["set"][j]
        output[pkey["Name"]] = pkeys_result[i]


def fill_foreign_keys(tables, table_schema, num_row, output, cstr: dict):
    fkeys = table_schema["FKeys"]
    if len(fkeys) == 1 and "key" in cstr:
        tabcol = cstr["key"][0]["extra"]["cols"][0]
        if tabcol[0] == table_schema["TableName"] and tabcol[1] == table_schema["FKeys"][0]["FName"]:
            cstr.pop("key")
            output[fkeys[0]["FName"]] = random.sample(
                tables[int(fkeys[0]["PTable"])][fkeys[0]["PName"]], k=num_row
            )
            return
    fkeys_results = defaultdict()
    for fkey in fkeys:
        # fkey is a part of pkey, so it has already been filled
        if fkey["FName"] in output:
            continue
        fkeys_results[fkey["FName"]] = random.choices(
            tables[int(fkey["PTable"])][fkey["PName"]], k=num_row
        )
    for fkey in fkeys:
        tabcol = table_schema["TableName"] + "." + fkey["FName"]
        if tabcol in cstr:
            c = cstr[tabcol][0]
            on_tab = c["with"].split(".")[0]
            on_col = c["with"].split(".")[1]
            if c["opt"] == "!=" and on_tab == table_schema["TableName"] and \
                on_col in [fkey["FName"] for fkey in fkeys]:
                on_col = fkeys_results[on_col]
                for i in range(num_row):
                    while fkeys_results[fkey["FName"]][i] == on_col[i]:
                        fkeys_results[fkey["FName"]][i] = \
                            random.choice(tables[int(fkey["PTable"])][fkey["PName"]])
    for fkey in fkeys:
        if fkey["FName"] not in output:
            output[fkey["FName"]] = fkeys_results[fkey["FName"]]


def fill_other_cols(fake: Faker, tables, table_schema, num_row, output, cstr: dict, tablename2index):
    others = table_schema["Others"]
    unique_percentage = random.uniform(0,1)
    firstcol_name = ""
    # find the depended col
    for col in others:
        tabcol = table_schema["TableName"] + "." + col["Name"]
        if tabcol in cstr:
            if cstr[tabcol][0]["opt"] in ["subset", "=>"]:
                on_tabcol = cstr[tabcol][0]["with"].split(".")
                if on_tabcol[0] == table_schema["TableName"] and \
                    on_tabcol[1] != col["Name"]:
                    firstcol_name = on_tabcol[1]
    # generate depended col
    for col in others:
        if col["Name"] == firstcol_name:
            output[col["Name"]] = gen_col(
                fake, col, unique_percentage, num_row
            )
    # generate other col
    for col in others:
        if col["Name"] not in output:
            tabcol = table_schema["TableName"] + "." + col["Name"]
            if tabcol in cstr:
                col_cstr = cstr[tabcol]
                if len(col_cstr[0]["with"]) != 0:
                    on_table = col_cstr[0]["with"].split(".")[0]
                    on_col = col_cstr[0]["with"].split(".")[1]
                    if on_table != table_schema["TableName"]:
                        if on_col in tables[tablename2index[on_table]].keys():
                            on_col = tables[tablename2index[on_table]][on_col]
                        else:
                            on_col = []
                    else:
                        if on_col in output.keys():
                            on_col = output[on_col]
                        else:
                            on_col = []
                    output[col["Name"]] = gen_col(
                        fake, col, unique_percentage, num_row, cstrs=col_cstr, on_col=on_col
                    )
                else:
                    output[col["Name"]] = gen_col(
                        fake, col, unique_percentage, num_row, cstrs=col_cstr
                    )
            else:
                output[col["Name"]] = gen_col(
                    fake, col, unique_percentage, num_row
                )

def random_gen_col(fake: Faker, col, unique_percentage: float, num_row):
    content = []
    _, gen_val, _ = type_functions(fake, col)
    content = [gen_val() for _ in range(num_row)]
    # potentially generate duplicates
    for i in range(num_row):
        if random.random() > unique_percentage:
            replaced = random.randrange(0, num_row)
            content[replaced] = content[i]
    return content

def is_satisfy(cstr, value):
    if cstr["opt"] == "<":
        if value < int(cstr["extra"]["value"]):
            return True
        else:
            return False
    elif cstr["opt"] == ">":
        if value > int(cstr["extra"]["value"]):
            return True
        else:
            return False
    elif cstr["opt"] == "=":
        if str(value) == cstr["extra"]["value"]:
            return True
        else:
            return False
    elif cstr["opt"] == "!=":
        if str(value) != cstr["extra"]["value"]:
            return True
        else:
            return False
    else:
        raise ValueError


def gen_col(fake: Faker, col, unique_percentage: float, num_row, cstrs: dict=[], on_col: list=[]):
    """Generate a column that is neither primary key nor foreign key"""
    content = []
    if len(cstrs) == 0:
        content = random_gen_col(fake, col, unique_percentage, num_row)
    else:
        for cstr in cstrs:
            if cstr["opt"] == "=>":
                if len(content) == 0:
                    content = random_gen_col(fake, col, unique_percentage, num_row)
                if len(on_col) == 0:
                    logging.info("fail to resolve implication relation")
                    raise ValueError
                for key in cstr["extra"]["if"].keys():
                    condition = cstr["extra"]["if"][key][0]
                for key in cstr["extra"]["then"].keys() :
                    result = cstr["extra"]["then"][key][0]
                for i, on_value in enumerate(on_col):
                    if is_satisfy(condition, on_value):
                        if result["opt"] == "=":
                            if result["extra"]["value"] == "null":
                                content[i] = None
                            else:
                                content[i] = result["extra"]["value"]
                        else:
                            _, gen_val, _ = type_functions(fake, col)
                            while not is_satisfy(result, content[i]):
                                content[i] = gen_val()
            elif cstr["opt"] == "subset":
                if len(on_col) == 0:
                    logging.info("fail to resolve subset relation")
                    raise ValueError
                content = random.choices(on_col, k=num_row)
            elif cstr["opt"] == ">" and len(cstr["with"]) != 0:
                if len(on_col) == 0:
                    content = random_gen_col(fake, col, unique_percentage, num_row)
                else:
                    content = []
                    col["Type"] += ",range"
                    for i in range(num_row):
                        col_str = str(on_col[i])
                        col_str = col_str.replace(" ", "-")
                        col_str = col_str.replace(":", "-")
                        col["Range"] = ","+col_str
                        _, gen_val, _ = type_functions(fake, col)
                        value = gen_val()
                        content.append(value)
            elif cstr["opt"] == "<" and len(cstr["with"]) != 0:
                if len(on_col) == 0:
                    content = random_gen_col(fake, col, unique_percentage, num_row)
                else:
                    content = []
                    col["Type"] += ",range"
                    for i in range(num_row):
                        col_str = str(on_col[i])
                        col_str.replace(" ", "-")
                        col_str.replace(":", "-")
                        col["Range"] = col_str+","
                        _, gen_val, _ = type_functions(fake, col)
                        value = gen_val()
                        content.append(value)
            elif cstr["opt"] == "!=" and len(cstr["with"]) != 0:
                content = random_gen_col(fake, col, unique_percentage, num_row)
                if len(on_col) != 0:
                    _, gen_val, _ = type_functions(fake, col)
                    for i, value in enumerate(content):
                        while value == on_col[i]:
                            value = gen_val()
            elif cstr["opt"] == "|":
                if len(content) == 0:
                    content = random_gen_col(fake, col, unique_percentage, num_row)
                else:
                    null_p = random.uniform(0,1)
                    indices = random.sample(list(range(num_row)), int(null_p*num_row))
                    for i in indices:
                        content[i] = None
            elif cstr["opt"] == "<-":
                if cstr["extra"]["discrete"]:
                    content = random.choices(cstr["extra"]["set"], k=num_row)
                else:
                    col["Type"] += ",range"
                    col["Range"] = cstr["extra"]["upper"] + "," + cstr["extra"]["lower"]
                    content = random_gen_col(fake, col, unique_percentage, num_row)
            elif cstr["opt"] == "->":
                if len(content) == 0:
                    content = random_gen_col(fake, col, unique_percentage, num_row)
                p = [random.uniform(0,1) for _ in range(len(cstr["extra"]["set"]))]
                p_total = random.uniform(0,1)
                p = [pi / sum(p) * p_total for pi in p]
                indices = [random.sample(list(range(num_row)), k=max(int(pi*num_row), 1)) for pi in p]
                for k, indice in enumerate(indices):
                    for i in indice:
                        content[i] = cstr["extra"]["set"][k]
            elif cstr["opt"] == "<":
                content = []
                if cstr["is_soft"]:
                    ncstr_num_row = int(random.uniform(0,1) * num_row)
                    content = random_gen_col(fake, col, unique_percentage, ncstr_num_row)
                    cstr_num_row = num_row - ncstr_num_row
                else:
                    cstr_num_row = num_row
                col["Type"] += ",range"
                col["Range"] = "," + cstr["extra"]["value"]
                content += random_gen_col(fake, col, unique_percentage, cstr_num_row)
            elif cstr["opt"] == ">":
                content = []
                if cstr["is_soft"]:
                    ncstr_num_row = int(random.uniform(0,1) * num_row)
                    content = random_gen_col(fake, col, unique_percentage, ncstr_num_row)
                    cstr_num_row = num_row - ncstr_num_row
                else:
                    cstr_num_row = num_row
                col["Type"] += ",range"
                col["Range"] = cstr["extra"]["value"] + ","
                content += random_gen_col(fake, col, unique_percentage, cstr_num_row)
            elif cstr["opt"] == "!=":
                content = random_gen_col(fake, col, unique_percentage, num_row)
                _, gen_val, _ = type_functions(fake, col)
                for value in content:
                    if random.uniform(0,1) > 0.5:
                        while value == cstr["extra"]["value"]:
                            value = gen_val()
                    else:
                        value = gen_val()
            elif cstr["opt"] == "=" and len(cstr["with"]) == 0:
                content = random_gen_col(fake, col, unique_percentage, num_row)
                _, gen_val, _ = type_functions(fake, col)
                for i in range(num_row):
                    if random.uniform(0,1) > 0.5:
                        content[i] = cstr["extra"]["value"]
            elif cstr["opt"] == "inc":
                content = list(range(1, num_row+1))
            elif cstr["opt"] == "consec":
                content = []
                content.append(0)
                for i in range(1, num_row):
                    d = random.uniform(0,1)
                    if d < 0.5:
                        content.append(content[i-1]+1)
                    else:
                        content.append(content[i-1])
            elif cstr["opt"] == "bound":
                content = random.sample(list(range(1,num_row+1)), num_row)
    return content



def refill_join_non_key(fake: Faker, tables, num_rows, join_cstrs, tab2idx, types):
    # If we just assume the "joining part" is the same number of ones for both column
    # then the number of rows to join for each table = sqrt(density * N1 * N2)
    for cstr, t in zip(join_cstrs, types):
        density = random.uniform(0,1)
        table1 = tab2idx[cstr["extra"]["tab1"]]
        table2 = tab2idx[cstr["extra"]["tab2"]]
        num_joining_rows = int(
            math.sqrt(float(density) * num_rows[table1] * num_rows[table2])
        )
        if t == "int":
            i_col = [1] * num_joining_rows + random.choices(
                list(range(1000, 2000)), k=num_rows[table1] - num_joining_rows
            )
            j_col = [1] * num_joining_rows + random.choices(
                list(range(3000, 4000)), k=num_rows[table2] - num_joining_rows
            )
        elif t == "varchar" or t == "string":
            i_num = num_rows[table1] - num_joining_rows
            j_num = num_rows[table2] - num_joining_rows
            words = [fake.unique.name() for k in range(i_num + j_num)]
            i_col = ["a"] * num_joining_rows + words[:i_num]
            j_col = ["a"] * num_joining_rows + words[i_num : i_num + j_num]
        tables[table1][cstr["extra"]["col1"]] = i_col
        tables[table2][cstr["extra"]["col1"]] = j_col


def enforce_duplicate(table_schema, num_row, output):
    # dup_table[0] = percent rows in table i to sample for repeating,
    # dup_table[1]: random.uniform(0.0, dup_table[1]) to determine number of rows to duplicate to
    # Only duplicate when no pkeys exist
    duplicate_range = [random.uniform(0,0.1) for _ in range(2)]
    if len(table_schema["PKeys"]) > 0:
        return
    rows_to_dup = random.sample(
        range(num_row), int(num_row * float(duplicate_range[0]))
    )
    # Random row numbers that will be sampled
    # print(len(rows_to_dup))
    total = len(rows_to_dup)
    count = 0
    for duplicate_row_num in rows_to_dup:
        count += 1
        duplicate_row = []
        # Populate duplicate_row
        for col_name in output:
            duplicate_row.append(output[col_name][duplicate_row_num])
        replaced_rows = random.sample(
            range(num_row),
            int(num_row * random.uniform(0.0, float(duplicate_range[1]))),
        )
        # Replace rows with duplicate_row
        for col_index, col_name in enumerate(output):
            for row in replaced_rows:
                output[col_name][row] = duplicate_row[col_index]
        if count % 100 == 0:
            print(count/total)


def type_functions(fake: Faker, col):
    """return (
         function converting str to type,
         function generating an instance of type
         function generating k unique values of type
    )
    """
    convert = str
    gen_val = gen_unique = lambda **kwargs: None
    lower, upper = None, None
    if "int" in col["Type"]:
        convert = int
        lower, upper = 0, 11000000
        if "range" in col["Type"]:
            if col["Range"][0] == ',':
                lower = 0
                upper = int(col["Range"][1:])
            elif col["Range"][-1] == ',':
                lower = int(col["Range"][0:-1])
                upper = int(col["Range"][0:-1]) + 11000000
            else:
                int_range = col["Range"].split(",")
                lower, upper = int(int_range[0]), int(int_range[1])
        gen_val = lambda lower=lower, upper=upper: random.randrange(lower, upper + 1)
        gen_unique = lambda k: random.sample(range(lower, upper + 1), k)

    elif "numeric" in col["Type"]:
        convert = float
        lower, upper = 0, 65535
        if "range" in col["Type"]:
            if col["Range"][0] == ',':
                lower = 0
                upper = int(col["Range"][1:])
            elif col["Range"][-1] == ',':
                lower = int(col["Range"][0:-1])
                upper = 65535
            else:
                float_range = col["Range"].split(",")
                lower, upper = float(float_range[0]), float(float_range[1])
        gen_val = lambda lower=lower, upper=upper: random.uniform(lower, upper)
        # duplicates are infrequent
        gen_unique = lambda k: [random.uniform(lower, upper) for _ in range(k)]

    elif "varchar" in col["Type"]:
        truncate = lambda string: string[
            : int(col["Size"]) if "size" in col["Type"] else None
        ]
        gen_val = lambda: truncate(fake.word())
        gen_unique = lambda k: [truncate(fake.unique.word()) for _ in range(k)]
    elif "enum" in col["Type"]:
        types = col["Type"].split(",")
        types.pop(0)
        gen_val = lambda: random.choice(types)
        gen_unique = lambda k: random.sample(types, min(k, len(types)))

    elif "time" in col["Type"] or "date" in col["Type"]:
        if "time" in col["Type"]:
            constructor = datetime.datetime
            cstr_field = 6
            date_between = fake.date_time_between
            unique_date_between = fake.unique.date_time_between
        else:
            constructor = datetime.date
            cstr_field = 3
            date_between = fake.date_between
            unique_date_between = fake.unique.date_between

        def convert(date_string):
            dt = [int(x) for x in date_string.split("-")[:cstr_field]]
            return constructor(*dt)

        lower = convert("1000-01-01")
        upper = convert("3000-12-30")
        if "range" in col["Type"]:
            if col["Range"][0] == ',':
                upper = convert(col["Range"][1:])
            elif col["Range"][-1] == ',':
                lower = convert(col["Range"][0:-1])
            else:
                date_range = col["Range"].split(",")
                lower = convert(date_range[0])
                upper = convert(date_range[1])

        def make_kwargs(lower=lower, upper=upper):
            kwargs = {}
            if lower:
                kwargs["start_date"] = lower
            if upper:
                kwargs["end_date"] = upper
            return kwargs

        gen_val = lambda **kwargs: date_between(**make_kwargs(**kwargs))
        gen_unique = lambda k: [unique_date_between(**make_kwargs()) for _ in range(k)]

    elif col["Type"] == "bool":
        gen_val = lambda: random.choice([True, False])
        gen_unique = lambda k: random.sample([True, False], min(k, 2))

    return convert, gen_val, gen_unique
