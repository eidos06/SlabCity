import secrets
from pglast.ast import *
from synthesizerv2.const import HolePriority
from pglast.visitors import Visitor
from pglast.printers import *


# A Base class for Hole
@node_printer("Hole", check_tags=False)
def print_hole(node, output):
    output.write("??")


class Hole(Node):
    __slots__ = ["level", "priority", "scope"]

    def __init__(self, level, priority, scope=set()):
        if not hasattr(self, "id"):
            self.id = uuid.uuid1()
        self.level = level
        self.priority = priority
        self.scope = scope


    def __str__(self):
        pass

    def __lt__(self, other):
        if self.level > other.level:
            return True
        if self.level < other.level:
            return False
        if self.priority < other.priority:
            return True
        if self.priority > other.priority:
            return False
        return False

    def __repr__(self):
        return self.__str__()


@node_printer("Hole_Rename", check_tags=False)
def print_hole_rename(node, output):
    output.write("??")

class Hole_Rename(Hole):
    __slots__ = []

    def __init__(self, level, scope=set()):
        super().__init__(level, HolePriority.Hole_Rename, scope)

    def __str__(self):
        return "?Rename?"

@node_printer("Hole_Rename_Content", check_tags=False)
def print_hole_rename_content(node, output):
    output.write("??")

class Hole_Rename_Content(Hole):
    __slots__ = []

    def __init__(self, level, scope=set()):
        super().__init__(level, HolePriority.Hole_Rename_Content, scope)

    def __str__(self):
        return "?Rename_Content?"

@node_printer("Hole_Rename_Renaming_List", check_tags=False)
def print_hole_rename_renaming_list(node, output):
    output.write("??")

class Hole_Rename_Renaming_List(Hole):
    __slots__ = []

    def __init__(self, level, scope=set()):
        super().__init__(level, HolePriority.Hole_Rename_Renaming_List, scope)

    def __str__(self):
        return "?Rename_Renaming_List?"

@node_printer("Hole_Target_List_No_Agg", check_tags=False)
def print_hole_target_list_no_agg(node, output):
    output.write("??")

class Hole_Target_List_No_Agg(Hole):
    __slots__ = []

    def __init__(self, level, scope=set()):
        super().__init__(level, HolePriority.Hole_Target_List_No_Agg, scope)

    def __str__(self):
        return "?Target_List_No_Agg?"

@node_printer("Hole_Target_List_With_Agg", check_tags=False)
def print_hole_target_list_with_agg(node, output):
    output.write("??")

class Hole_Target_List_With_Agg(Hole):
    __slots__ = []

    def __init__(self, level, scope=set()):
        super().__init__(level, HolePriority.Hole_Target_List_With_Agg, scope)

    def __str__(self):
        return "?Target_List_With_Agg?"

@node_printer("Hole_Column_List", check_tags=False)
def print_hole_column_list(node, output):
    output.write("??")

class Hole_Column_List(Hole):
    __slots__ = []

    def __init__(self, level, scope=set()):
        super().__init__(level, HolePriority.Hole_Column_List, scope)

    def __str__(self):
        return "?Column_List?"

@node_printer("Hole_Target_List_Item_With_Agg", check_tags=False)
def print_hole_target_list_item_with_agg(node, output):
    output.write("??")

class Hole_Target_List_Item_With_Agg(Hole):
    __slots__ = []

    def __init__(self, level, scope=set()):
        super().__init__(level, HolePriority.Hole_Target_List_Item_With_Agg, scope)

    def __str__(self):
        return "?Target_List_Item_With_Agg?"

@node_printer("Hole_Target_List_Item_No_Agg", check_tags=False)
def print_hole_target_list_item_no_agg(node, output):
    output.write("??")

class Hole_Target_List_Item_No_Agg(Hole):
    __slots__ = []

    def __init__(self, level, scope=set()):
        super().__init__(level, HolePriority.Hole_Target_List_Item_No_Agg, scope)

    def __str__(self):
        return "?Target_List_Item_No_Agg?"

@node_printer("Hole_Column", check_tags=False)
def print_hole_column(node, output):
    output.write("??")

class Hole_Column(Hole):
    __slots__ = []

    def __init__(self, level, scope=set()):
        super().__init__(level, HolePriority.Hole_Column, scope)

    def __str__(self):
        return "?Column?"

@node_printer("Hole_Func", check_tags=False)
def print_hole_func(node, output):
    output.write("??")

class Hole_Func(Hole):
    __slots__ = []

    def __init__(self, level, scope=set()):
        super().__init__(level, HolePriority.Hole_Func, scope)

    def __str__(self):
        return "?Function?"

@node_printer("Hole_Expr", check_tags=False)
def print_hole_expr(node, output):
    output.write("??")

class Hole_Expr(Hole):
    __slots__ = []

    def __init__(self, level, scope=set()):
        super().__init__(level, HolePriority.Hole_Expr, scope)

    def __str__(self):
        return "?Expression?"

@node_printer("Hole_Where_Predicate", check_tags=False)
def print_hole_where_predicate(node, output):
    output.write("??")

class Hole_Where_Predicate(Hole):
    __slots__ = []

    def __init__(self, level, scope=set()):
        super().__init__(level, HolePriority.Hole_Where_Predicate, scope)

    def __str__(self):
        return "?Where_Predicate?"

@node_printer("Hole_Having_Predicate", check_tags=False)
def print_hole_having_predicate(node, output):
    output.write("??")

class Hole_Having_Predicate(Hole):
    __slots__ = []

    def __init__(self, level, scope=set()):
        super().__init__(level, HolePriority.Hole_Having_Predicate, scope)

    def __str__(self):
        return "?Having_Predicate?"

@node_printer("Hole_Join_On_Predicate", check_tags=False)
def print_hole_join_on_predicate(node, output):
    output.write("??")

class Hole_Join_On_Predicate(Hole):
    __slots__ = []

    def __init__(self, level, scope=set()):
        super().__init__(level, HolePriority.Hole_Join_On_Predicate, scope)

    def __str__(self):
        return "?Join_On_Predicate?"
