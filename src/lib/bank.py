from __future__ import annotations
from typing import TYPE_CHECKING
from collections import Counter
from lib.envrionments import ConstraintEnvrionment
from lib.cost_estimater import CostEstimater
if TYPE_CHECKING:
    from typing import Dict, Set, List
    from lib.dsl import SQLComponent


class Bank:
    __slots__ = ["_bank_dic"]

    def __init__(self, component_list: List[SQLComponent] = None):
        if component_list is None:
            self._bank_dic: Dict[type, Set[SQLComponent]] = {}
        else:
            self._bank_dic: Dict[type, Set[SQLComponent]] = {}
            for i in component_list:
                self.add_to_bank(i)

    def add_to_bank(self, item: SQLComponent):
        class_set = self._bank_dic.get(item.__class__, None)
        if class_set is None:
            self._bank_dic[item.__class__] = {item}
        else:
            class_set.add(item)

    def get_from_bank(self, cls: type, cost: int, cost_model: Counter,
                      constraint: ConstraintEnvrionment = ConstraintEnvrionment()) -> List[SQLComponent]:
        if cost < 0:
            return []
        result = set()
        for k, v in self._bank_dic.items():
            if issubclass(k, cls):
                result = result.union(v)
        final_output = []
        for i in result:
            if CostEstimater.get_cost(query=i, cost_model=cost_model) > cost:
                continue
            if constraint.verify(i):
                final_output.append(i)
        return final_output

    def merge(self, other_bank: Bank):
        for key, item in other_bank._bank_dic.items():
            if key not in self._bank_dic.keys():
                self._bank_dic[key] = item
            else:
                self._bank_dic[key] = self._bank_dic[key].union(item)

    def __contains__(self, item):
        if item.__class__ in self._bank_dic:
            s = self._bank_dic[item.__class__]
            return item in s
        else:
            return False

    def __len__(self):
        return sum([len(i) for i in self._bank_dic.values()])

    def empty(self):
        return len(self._bank_dic) == 0

    def __str__(self):
        result = ""
        for k, v in self._bank_dic.items():
            result += f"{k.__name__} :[" + ", ".join([str(i) for i in v]) + "]\n"
        return result
