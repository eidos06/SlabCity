from __future__ import annotations
from typing import TYPE_CHECKING, List, Dict
from collections import Counter
from lib.basics import AliasDicKey

if TYPE_CHECKING:
    from lib.dsl import SQLComponent, Chain
    from lib.constraints import Constraint


class CacheEnvrionment:
    __slots__ = ["hash", "cost", "component_count", "all_component"]

    def __init__(self):
        self.hash: int | None = None
        self.cost: int | None = None
        self.component_count: Counter | None = None
        self.all_component :Set[SQLComponent] | None = None


class ConcretizeEnvrionment:
    __slots__ = ["_dic"]

    def __init__(self):
        self._dic: Dict[Chain, List[AliasDicKey]] = {}

    def add_link(self, chain: Chain, name: AliasDicKey):
        l = self._dic.get(chain, None)
        if l is None:
            self._dic[chain] = [name]
        else:
            l.append(name)

    def get_link(self, chain: Chain) -> List[AliasDicKey]:
        return self._dic.get(chain, [])

    def merge(self, other: ConcretizeEnvrionment) -> ConcretizeEnvrionment:
        result: ConcretizeEnvrionment = ConcretizeEnvrionment()
        for k, v in self._dic.items():
            for name in v:
                result.add_link(k, name)
        for k, v in other._dic.items():
            for name in v:
                result.add_link(k, name)
        return result

    def update_table_alias(self, table_alias: str):
        result: ConcretizeEnvrionment = ConcretizeEnvrionment()
        for k, v in self._dic.items():
            for name in v:
                result.add_link(k, AliasDicKey(table_alias, name.column))
        return result


class ConstraintEnvrionment:
    __slots__ = ["constraints"]

    def __init__(self, constraints: List[Constraint] = None):
        if constraints is None:
            constraints = []
        self.constraints: List[Constraint] = constraints

    def verify(self, component: SQLComponent) -> bool:
        for i in self.constraints:
            if not i.verify(component):
                return False
        return True
