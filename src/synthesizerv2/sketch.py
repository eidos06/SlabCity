import math
from pglast.visitors import Visitor
from synthesizerv2.hole import Hole
from pglast.ast import *
from pglast.stream import IndentedStream
from synthesizerv2.basics import deepcopy



class Sketch:
    __slots__ = ["query", "holes", "changed_from_previous_cost_calculation", "cost", "change_num", "filter_indicator", "filter_location", "hole_filters_dict"]

    def __init__(self, query):
        self.query = query
        self.holes = HoleGetter()(query)
        self.changed_from_previous_cost_calculation = False
        self.cost = 0
        self.change_num = 0

        self.filter_indicator = []
        self.filter_location = []
        # key: hole_id; value: list of filters
        self.hole_filters_dict = dict()
    
    def __hash__(self) -> int:
        class PrintVisitor(Visitor):
            def __init__(self):
                self.dic = {}

            def visit(self, ancestors, node):
                if isinstance(node, FreshCol):
                    if node.col_index in self.dic:
                        return FreshCol(self.dic[node.col_index])
                    else:
                        new_id = len(self.dic)
                        self.dic[node.col_index] = new_id
                        return FreshCol(new_id)
            def __call__(self, query):
                super().__call__(query)
                # print(self.dic)
                return query

        node1_copy = deepcopy(self.query)
        node1_copy = PrintVisitor()(node1_copy)
        str1 = IndentedStream()(node1_copy)
        return hash(str1)
    
    def __eq__(self, __o: object) -> bool:
        return hash(self) == hash(__o)
    def __lt__(self, other):
        return self.get_cost() < other.get_cost()

    def get_filters_by_id(self, hole_id):
        if hole_id not in self.hole_filters_dict:
            return []
        return self.hole_filters_dict[hole_id]

    def calculate_cost(self):
        return self.change_num * len(self.holes)

    def get_cost(self):
        if self.changed_from_previous_cost_calculation:
            self.cost = self.calculate_cost()
            self.changed_from_previous_cost_calculation = False
        return self.cost

    def filled(self):
        return len(self.holes) == 0


    def replace_hole(self, hole, new_node):
        Replacer()(self, hole, new_node)
        self.holes = HoleGetter()(self.query)
        self.changed_from_previous_cost_calculation = True
        self.change_num = self.change_num + 1

    def pick_hole(self):
        return min(self.holes)


class Replacer(Visitor):
    def __call__(self, sketch: Sketch, hole: Hole, fill_node: Node):
        self.hole_to_be_filled = hole
        self.node_to_be_filled = fill_node
        self.query = sketch.query
        super().__call__(self.query)
        sketch.query = self.query
        return sketch

    def visit(self, ancestors, node):
        if isinstance(node, Hole):
            if node.id == self.hole_to_be_filled.id:
                return self.node_to_be_filled


class HoleGetter(Visitor):
    def __call__(self, query):
        self.holes = []
        super().__call__(query)
        return self.holes

    def visit(self, ancestors, node):
        if isinstance(node, Hole):
            self.holes.append(node)
