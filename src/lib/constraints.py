from __future__ import annotations
from typing import Tuple, Set, List, TYPE_CHECKING


if TYPE_CHECKING:
    from lib.dsl import PredicateOpClause, PredicateClause, SQLComponent, Chain, PredicateBinOpLogicClause, \
        ReferencableChain
from lib.types import DataType


class Constraint:
    def verify(self, component: SQLComponent) -> bool:
        raise NotImplementedError("the constraint doesn't have verify function implemented")


class FulfilledByProvidedChainsConstraint(Constraint):
    __slots__ = ["provided_chains"]

    def __init__(self, provided_chains: Tuple[Chain, ...]):
        self.provided_chains: Set[Chain] = set(provided_chains)

    def verify(self, component: SQLComponent) -> bool:
        return set(component.required_chain).issubset(self.provided_chains)


class PredicateTwoSideDifferentSourceConstraint(Constraint):
    def __init__(self, left_source: Tuple[Chain], right_source: Tuple[Chain]):
        self.left_source = set(left_source)
        self.right_source = set(right_source)

    def verify(self, component: SQLComponent) -> bool:
        from lib.dsl import PredicateOpClause, PredicateClause, PredicateBinOpLogicClause
        assert isinstance(component, PredicateClause)
        if isinstance(component, PredicateOpClause):
            required_left = set(component.larg.required_chain)
            required_right = set(component.rarg.required_chain)
            return (required_left.issubset(self.left_source) and required_right.issubset(self.right_source)) \
                   or required_left.issubset(self.right_source) and required_right.issubset(self.left_source)
        if isinstance(component, PredicateBinOpLogicClause):
            return self.verify(component.larg) and self.verify(component.rarg)
        raise Exception("unrecognized predicate clause")


class NotInConstraint(Constraint):
    def __init__(self, black_list: List[SQLComponent]):
        self.black_list = black_list

    def verify(self, component: SQLComponent) -> bool:
        return component not in self.black_list


class DataTypeInConstraint(Constraint):
    def __init__(self, allowed_types: List[DataType]):
        self.allowed_data_types = allowed_types

    def verify(self, component: SQLComponent) -> bool:
        return component.data_type in self.allowed_data_types


class LogicExpressionNoDuplicates(Constraint):
    def __init__(self, template: SQLComponent):
        self.template_set = set(self.collect_predicate_op(template))

    def collect_predicate_op(self, input_component: SQLComponent) -> List[PredicateOpClause]:
        from lib.dsl import PredicateOpClause
        if isinstance(input_component, PredicateOpClause):
            return [input_component]
        from lib.dsl import PredicateBinOpLogicClause
        if isinstance(input_component, PredicateBinOpLogicClause):
            return self.collect_predicate_op(input_component.larg) + self.collect_predicate_op(input_component.rarg)
        return []

    def verify(self, component: SQLComponent) -> bool:
        to_be_verified = self.collect_predicate_op(component)
        return self.template_set.isdisjoint(to_be_verified)


class NotNoneConstraint(Constraint):
    def verify(self, component: SQLComponent) -> bool:
        from lib.dsl import NoneClause
        return not isinstance(component, NoneClause)


class NoAggClauseInPredicateConstraint(Constraint):
    def verify(self, component: SQLComponent) -> bool:
        from lib.dsl import PredicateOpClause, AggregationClause, PredicateBinOpLogicClause
        if isinstance(component, AggregationClause):
            return False
        if isinstance(component, PredicateOpClause):
            return self.verify(component.larg) and self.verify(component.rarg)
        if isinstance(component, PredicateBinOpLogicClause):
            return self.verify(component.larg) and self.verify(component.rarg)
        return True


class AggregationOnlyConstraint(Constraint):
    def verify(self, component: SQLComponent) -> bool:
        from lib.dsl import AggregationClause
        if isinstance(component, AggregationClause):
            return True
        return False


class NonAggregationOnlyConstraint(Constraint):
    def verify(self, component: SQLComponent) -> bool:
        from lib.dsl import AggregationClause
        if isinstance(component, AggregationClause):
            return False
        return True


class ColumnEitherInGroupByOrAggregationConstraint(Constraint):
    def __init__(self, chains_in_group_by: Tuple[ReferencableChain]):
        self.chains_in_group_by = chains_in_group_by

    def verify(self, component: SQLComponent):
        from lib.dsl import AggregationClause, PredicateOpClause, PredicateBinOpLogicClause, ReferencableChain
        if isinstance(component, AggregationClause):
            return True
        if isinstance(component, ReferencableChain):
            return component in self.chains_in_group_by
        if isinstance(component, PredicateOpClause):
            return self.verify(component.larg) and self.verify(component.rarg)
        if isinstance(component, PredicateBinOpLogicClause):
            return self.verify(component.larg) and self.verify(component.rarg)
        return True


class MaxDepthConstraint(Constraint):
    def __init__(self, max_depth):
        self.max_depth = max_depth

    def verify(self, component: SQLComponent) -> bool:
        return component.depth <= self.max_depth



