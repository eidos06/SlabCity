from pglast.ast import *

class Filter:
    __slots__ = ["content", "scope", "id"]

    def __init__(self, content, scope):
        if not hasattr(self, "id"):
            self.id = uuid.uuid1()
        self.content = content
        self.scope = scope

    # def __eq__(self, __o: object) -> bool:
    #     pass

    # def __hash__(self) -> int:
    #     pass

    def __eq__(self, __o: object) -> bool:
        return set(self.content) == set(__o.content) and self.scope == __o.scope

    def __hash__(self) -> int:
        return hash((frozenset(self.content), frozenset(self.scope)))


class Predicate_Filter(Filter):
    __slots__ = ["has_agg"]

    def __init__(self, content, scope, has_agg):
        super().__init__(content, scope)
        self.has_agg = has_agg


class Group_Filter(Filter):
    __slots__ = []


class Join_Condition_Filter(Filter):
    __slots__ = []