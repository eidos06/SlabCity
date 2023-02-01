from pglast.ast import *
from pglast.visitors import Visitor
from pglast.stream import IndentedStream
from utils.baked_benchmark_getter import BenchmarkQuery
from utils.deepcopy import deepcopy
from typing import List


def get_query_hash(query: Node, consider_sort_order: bool = True) -> int:
    """
    Get the hash of a query for comparison usage.

    Args:
        query: (Required) the query to be hashed
        consider_sort_order: (Optional) if the hash should consider ORDER BY case

    Returns:
        int: the hash of that query

    """
    class PrintVisitor(Visitor):
        def __init__(self, consider_sort_order=True):
            self.dic = {}
            self.consider_sort_order = consider_sort_order

        def visit(self, ancestors, node):
            if isinstance(node, FreshCol):
                if node.col_index in self.dic:
                    return FreshCol(self.dic[node.col_index])
                else:
                    new_id = len(self.dic)
                    self.dic[node.col_index] = new_id
                    return FreshCol(new_id)
            if isinstance(node, A_Expr):
                try:
                    if node.name is not None:
                        # symmetric operators
                        if node.name[0].val in ["=", "!=", "<>", "+", "*"]:
                            if node.lexpr is not None and node.rexpr is not None:
                                left_hash = hash(IndentedStream()(node.lexpr))
                                right_hash = hash(IndentedStream()(node.rexpr))
                                if right_hash > left_hash:
                                    tmp = node.lexpr
                                    node.lexpr = node.rexpr
                                    node.rexpr = tmp
                        # asymmetric operators
                        if node.name[0].val in [">", ">=", "<=", "<"]:
                            if node.lexpr is not None and node.rexpr is not None:
                                left_hash = hash(IndentedStream()(node.lexpr))
                                right_hash = hash(IndentedStream()(node.rexpr))
                                if right_hash > left_hash:
                                    tmp = node.lexpr
                                    if node.name[0].val == ">":
                                        node.name[0].val = "<"
                                    elif node.name[0].val == ">=":
                                        node.name[0].val = "<="
                                    elif node.name[0].val == "<":
                                        node.name[0].val = ">"
                                    elif node.name[0].val == "<=":
                                        node.name[0].val = ">="
                                    node.lexpr = node.rexpr
                                    node.rexpr = tmp


                except:
                    print("error from query comparison: ")
                    print(node)
                    pass
            if not self.consider_sort_order:
                if isinstance(node, SelectStmt):
                    if node.sortClause:
                        node.sortClause = None

        def __call__(self, query):
            super().__call__(query)
            # print(self.dic)
            return query

    node1_copy = deepcopy(query)
    node1_copy = PrintVisitor(consider_sort_order=consider_sort_order)(node1_copy)
    str1 = IndentedStream()(node1_copy)
    # print(str1)
    return hash(str1)


def is_query_has_equiv_in_list(l_generated: List[Node], l_ground_truth: List[BenchmarkQuery],
                               consider_sort_order=True):
    result = []
    result_idx = []
    origin_hash = {}

    for idx, l_o in enumerate(l_generated):
        origin_hash[get_query_hash(l_o, consider_sort_order=consider_sort_order)] = idx
    for l_g in l_ground_truth:
        l_g_hash = get_query_hash(l_g.query, consider_sort_order=consider_sort_order)
        if l_g_hash in origin_hash:
            result.append(l_g)
            result_idx.append(origin_hash[l_g_hash])
    return result, result_idx


def is_two_queries_identical(node1: Node, node2: Node):
    return get_query_hash(node1) == get_query_hash(node2)


