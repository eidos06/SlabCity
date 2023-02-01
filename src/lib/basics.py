from __future__ import annotations
import pickle
import time
from typing import Dict, Tuple, TYPE_CHECKING
from pglast.ast import String
from lib.exceptions import *

if TYPE_CHECKING:
    from lib.dsl import Table, Chain


class AliasDicKey:
    __slots__ = ["table", "column"]

    def __init__(self, table: str, column: str):
        self.table = table
        self.column = column

    def __hash__(self):
        return hash(self.table + "." + self.column)

    def __str__(self):
        return self.table + "." + self.column

    def __eq__(self, other):
        return self.table == other.table and self.column == other.column


class AliasDic:
    __slots__ = ["_dic"]

    def __init__(self):
        self._dic: Dict[AliasDicKey, Chain] = {}

    def record_alias(self, alias: str, table_namespace: str, reference: Chain):
        self._dic[AliasDicKey(table_namespace.lower(), alias.lower())] = reference

    def get_reference_by_alias(self, alias: Tuple[String]):
        if len(alias) == 1:
            search_key = alias[0].val.lower()
            search_result = []
            for k, v in self._dic.items():
                if k.column == search_key:
                    search_result.append(v)
            if len(search_result) == 0:
                raise AliasTranslationException("can't find key name: " + str(alias))
            if len(search_result) > 1:
                raise AliasTranslationException("multiple key name: " + str(alias))
            return search_result[0]
        elif len(alias) == 2:
            k = AliasDicKey(alias[0].val.lower(), alias[1].val.lower())
            reference = self._dic.get(k, None)
            if reference is None:
                raise AliasTranslationException("can't find key name: " + str(alias))
            return reference
        raise AliasTranslationException("format of alias is wrong")

    def get_full_alias(self, alias: tuple[String]) -> Tuple[str, str]:
        if len(alias) == 1:
            search_key = alias[0].val.lower()
            search_result = []
            for k, v in self._dic.items():
                if k.column == search_key:
                    search_result.append(k)
            if len(search_result) == 0:
                raise AliasTranslationException("can't find key name: " + str(alias))
            if len(search_result) > 1:
                raise AliasTranslationException("multiple key name: " + str(alias))
            return search_result[0].table, search_result[0].column
        elif len(alias) == 2:
            search_key = AliasDicKey(alias[0].val.lower(), alias[1].val.lower())
            search_result = self._dic.get(search_key, None)
            if search_result is not None:
                return search_key.table, search_key.column
        raise AliasTranslationException("format of alias is wrong")

    def get_items(self):
        return self._dic.items()

    def combine(self, other):
        self._dic.update(other._dic)


class Schema:
    __slots__ = ["_tables"]

    def __init__(self):
        self._tables: Dict[str, Table] = {}

    def add_table(self, schema: Table):
        self._tables[schema.table_name.lower()] = schema
        return self

    def get_table(self, table_name: str) -> Table:
        return self._tables[table_name.lower()]

    def get_table_names(self):
        return list(self._tables.keys())

    def __str__(self):
        result = ""
        result += "Schema:\n"
        for k, v in self._tables.items():
            result += f"{k}({v.unique_identifier}) - {str(v.table_cols)}\n"
        return result

    @property
    def tables(self):
        return self._tables

class AliasDicForPreProcess:
    __slots__ = ["_dic"]

    def __init__(self):
        self._dic: Dict[AliasDicKey, None] = {}

    def record_alias(self, alias: str, table_namespace: str):
        self._dic[AliasDicKey(table_namespace, alias)] = "-1"

    def get_reference_by_alias(self, alias: Tuple[String]):
        if len(alias) == 1:
            search_key = alias[0].val
            search_result = []
            for k, v in self._dic.items():
                if k.column == search_key:
                    search_result.append(v)
            if len(search_result) == 0:
                raise AliasTranslationException("can't find key name: " + str(alias))
            if len(search_result) > 1:
                raise AliasTranslationException("multiple key name: " + str(alias))
            return search_result[0]
        elif len(alias) == 2:
            k = AliasDicKey(alias[0].val, alias[1].val)
            reference = self._dic.get(k, None)
            if reference is None:
                raise AliasTranslationException("can't find key name: " + str(alias))
            return reference
        raise AliasTranslationException("format of alias is wrong")

    def get_full_alias(self, alias: tuple[String]) -> Tuple[str, str]:
        if len(alias) == 1:
            search_key = alias[0].val
            search_result = []
            for k, v in self._dic.items():
                if k.column == search_key:
                    search_result.append(k)
            if len(search_result) == 0:
                raise AliasTranslationException("can't find key name: " + str(alias))
            if len(search_result) > 1:
                raise AliasTranslationException("multiple key name: " + str(alias))
            return search_result[0].table, search_result[0].column
        elif len(alias) == 2:
            search_key = AliasDicKey(alias[0].val, alias[1].val)
            search_result = self._dic.get(search_key, None)
            if search_result is not None:
                return search_key.table, search_key.column
        raise AliasTranslationException("format of alias is wrong")

    def get_items(self):
        return self._dic.items()

    def get_first_available_column(self):
        first_key: AliasDicKey = self._dic.keys()[0]
        if first_key.table == "":
            return ColumnRef(fields=tuple([String(first_key.column)]))
        else:
            return ColumnRef(fields=tuple([String(first_key.table), String(first_key.column)]))

    def get_all_available_column(self) -> List[ColumnRef]:
        result = []
        for key in self._dic.keys():
            if key.table == "":
                result.append(ColumnRef(fields=tuple([String(key.column)])))
            else:
                result.append(ColumnRef(fields=tuple([String(key.table), String(key.column)])))
        return result

    def combine(self, other):
        self._dic.update(other._dic)


class TimeoutController:
    __slots__ = ["_start_timestamp", "_timeout_in_sec"]

    def __init__(self, timeout_in_sec: int):
        self._start_timestamp = time.perf_counter()
        self._timeout_in_sec = timeout_in_sec

    def timeout(self) -> bool:
        current_timeout = time.perf_counter()
        if current_timeout - self._start_timestamp > self._timeout_in_sec:
            return True
        return False


def deepcopy(item):
    copier = getattr(item, "__deepcopy__", None)
    if copier is not None:
        return copier(item)
    else:
        return pickle.loads(pickle.dumps(item))
    # return copy.deepcopy(item)

class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
