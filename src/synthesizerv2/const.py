from enum import IntEnum, auto


class HolePriority:
    Hole_Rename = 1
    Hole_Rename_Content = 1

    Hole_Input_Table = 1

    Hole_Column = 5
    Hole_Func = 5
    Hole_Expr = 5
    Hole_Where_Predicate = 5
    Hole_Having_Predicate = 5
    Hole_Join_On_Predicate = 5
    Hole_Column_List = 5
    Hole_Op = 5

    Hole_Target_List_With_Agg = 10
    Hole_Target_List_Item_With_Agg = 10
    Hole_Target_List_No_Agg = 10
    Hole_Target_List_Item_No_Agg = 10

    Hole_Rename_Renaming_List = 100

class Limit:
    Cost_Upperbound = 100
    Nesting_Search_Bound = 100
    Target_List_Length = 2


