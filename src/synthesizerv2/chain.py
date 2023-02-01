from typing import List
from pglast.ast import *
from synthesizerv2.basics import *


class ChainItem:
    def __str__(self):
        pass

    def __repr__(self):
        return self.__str__()

    def __eq__(self, other):
        pass

    def __hash__(self) -> int:
        pass


class FunctionChainItem(ChainItem):
    def __init__(self, funcname):
        self.funcname = funcname

    def __str__(self):
        return self.funcname

    def __eq__(self, other):
        return isinstance(other, FunctionChainItem) and self.funcname == other.funcname

    def __deepcopy__(self, memodict={}):
        return FunctionChainItem(self.funcname)

    def __hash__(self) -> int:
        return hash((self.funcname))


class FreshColChainItem(ChainItem):
    def __init__(self, index, pointer=None):
        self.index = index
        self.pointer = pointer

    def __str__(self):
        return "c" + str(self.index)

    def __eq__(self, other):
        return isinstance(other, FreshColChainItem) and self.index == other.index

    def __deepcopy__(self, memodict={}):
        return FreshColChainItem(self.index, deepcopy(self.pointer))


class BaseColumnChainItem(ChainItem):
    def __init__(self, table, column):
        self.table = table
        self.column = column

    def __str__(self):
        return self.table + "." + self.column

    def __eq__(self, other):
        return isinstance(other, BaseColumnChainItem) and self.table == other.table \
               and self.column == other.column
    def __deepcopy__(self, memodict={}):
        return BaseColumnChainItem(self.table, self.column)

    def __hash__(self) -> int:
        return hash((self.table, self.column))


class Chain:
    def __init__(self, content=None):
        if content is None:
            content = []
        self.chain:List[ChainItem] = content

    def add(self, item: ChainItem):
        self.chain.append(item)
        return self

    def __str__(self):
        if len(self.chain) == 0:
            return ""
        output = ""
        for i in range(len(self.chain) - 1):
            output += str(self.chain[i]) + " -> "
        output += str(self.chain[len(self.chain) - 1])
        return output

    def __len__(self):
        return len(self.chain)

    def to_compact_form(self):
        new_chain = Chain()
        for item in self.chain:
            if not isinstance(item, FreshColChainItem):
                new_chain.add(item)
        return new_chain

    def include_functions(self):
        for item in self.chain:
            if isinstance(item, FunctionChainItem):
                return True
        return False

    def __repr__(self):
        return self.__str__()

    def __eq__(self, other):
        if len(self.chain) != len(other.chain):
            return False
        for i in range(len(self.chain)):
            if self.chain[i] != other.chain[i]:
                return False
        return True

    def __deepcopy__(self, memodict={}):
        new_chain_list = []
        for i in self.chain:
            new_chain_list.append(deepcopy(i))
        return Chain(new_chain_list)

    def __hash__(self) -> int:
        return hash(frozenset(self.chain))


"""
check if Chain a is the prefix of Chain b
"""


def is_chain_prefix(a: Chain, b: Chain):
    if len(a.chain) > len(b.chain):
        return False
    for i in range(len(a.chain)):
        if a.chain[i] != b.chain[i]:
            return False
    return True


"""
cur chain list a by chain list b. The returned list is a list of chains after cutting.
The returned list should be the same size as a
"""


def cut_chains(a: List[Chain], b: List[Chain]):
    results = []
    for to_cut in a:
        to_cut = to_cut.to_compact_form()
        after_cut = to_cut
        for cutter in b:
            cutter = cutter.to_compact_form()
            if is_chain_prefix(cutter, to_cut):
                tmp = Chain(to_cut.chain[len(cutter.chain):])
                if len(tmp) < len(after_cut):
                    after_cut = tmp
        results.append(after_cut)
    return results


def cut_chain(a: Chain, b: Chain):
    a_compact = a.to_compact_form()
    b_compact = b.to_compact_form()
    if not is_chain_prefix(b_compact, a_compact):
        raise Exception("Can not cut two chains which don't have prefix relationship")
    after_cut = Chain(a_compact.chain[len(b_compact.chain):])
    return after_cut


"""

"""


def _chain_extend_helper_output_node(original_chain: Chain, to_extend: Chain):
    if len(to_extend) == 0:
        if isinstance(original_chain.chain[-1], FreshColChainItem):
            return [[original_chain.chain[-1].pointer, original_chain]]
    if len(to_extend) == 1:
        if isinstance(original_chain.chain[-1], FreshColChainItem):
            if isinstance(to_extend.chain[0], FunctionChainItem):
                new_chain = deepcopy(original_chain).add(to_extend.chain[0])
                return [[FuncCall(funcname=tuple([String(to_extend.chain[0].funcname)]), args=tuple([original_chain.chain[-1].pointer])), new_chain]]
            raise "Shouldn't get here - in _chain_extend_helper"
        raise "Shouldn't get here - in _chain_extend_helper"


def get_possible_extension_for_chain(original_chain: Chain, longer_chain: Chain):
    result = []
    original_chain_compact = original_chain.to_compact_form()
    if not is_chain_prefix(original_chain_compact, longer_chain):
        return []
    chain_cut_result = cut_chain(longer_chain, original_chain_compact)
    for i in range(min(2, len(chain_cut_result) + 1)):
        result += _chain_extend_helper_output_node(original_chain, Chain(chain_cut_result.chain[0:i]))
    return result
