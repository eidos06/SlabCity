from __future__ import annotations

from collections import Counter
from typing import Tuple, List, Dict, Set

from lib.analysis_info import ComponentAnalysisInfo
from lib.bank import Bank
from lib.basics import TimeoutController
from lib.constraints import FulfilledByProvidedChainsConstraint, PredicateTwoSideDifferentSourceConstraint, \
    NotInConstraint, DataTypeInConstraint, LogicExpressionNoDuplicates, NotNoneConstraint, \
    NoAggClauseInPredicateConstraint, ColumnEitherInGroupByOrAggregationConstraint, \
    AggregationOnlyConstraint, NonAggregationOnlyConstraint, MaxDepthConstraint
from lib.cost_estimater import CostEstimater
from lib.envrionments import ConcretizeEnvrionment, CacheEnvrionment
from lib.types import DataType
from lib.envrionments import ConstraintEnvrionment
from utils.algorithms import get_all_combinations_below_n
from lib.basics import AliasDicKey
import lib.consts

import lib.consts

"""
This file contains language definition.
"""

"""
Some helper functions
"""


class UniqueNameGenerator:
    hints: Dict[str, int] = {}

    @classmethod
    def reset(cls):
        cls.hints = {}

    @classmethod
    def get_unique_num_from_hint(cls, hint: str) -> str:
        num = cls.hints.get(hint, 0)
        cls.hints[hint] = num + 1
        return str(num)

    @classmethod
    def generate_table_name(cls, hint: str = ""):
        hint = "T_" + hint
        idx = cls.get_unique_num_from_hint(hint)
        return f"{hint}_{idx}"

    @classmethod
    def generate_col_name(cls, hint: str = ""):
        if hint != "":
            hint = "c_" + hint
            idx = cls.get_unique_num_from_hint(hint)
            return f"{hint}{idx}"
        else:
            idx = cls.get_unique_num_from_hint(hint)
            return f"c_{idx}"


class UniqueNameGeneratorForConcretize:
    hints: Dict[str, int] = {}

    @classmethod
    def reset(cls):
        cls.hints = {}

    @classmethod
    def get_unique_num_from_hint(cls, hint: str) -> str:
        num = cls.hints.get(hint, 0)
        cls.hints[hint] = num + 1
        return str(num)

    @classmethod
    def generate_table_name(cls, hint: str = ""):
        hint = "T_" + hint
        idx = cls.get_unique_num_from_hint(hint)
        return f"{hint}_{idx}"

    @classmethod
    def generate_col_name(cls, hint: str = ""):
        if hint != "":
            hint = "c_" + hint
            idx = cls.get_unique_num_from_hint(hint)
            return f"{hint}{idx}"
        else:
            idx = cls.get_unique_num_from_hint(hint)
            return f"c_{idx}"


"""
The base component

Each component should have followings:
1. CacheEnvrionment, where stored cached cost and hash
2. ComponentAnalysisInfo, where stored required chains, provided chains and component counter
"""


class SQLComponent:
    __slots__ = ["_cache", "_analysis_info"]

    def __init__(self, analysis_info: ComponentAnalysisInfo):
        self._analysis_info: ComponentAnalysisInfo = analysis_info
        self._cache: CacheEnvrionment = CacheEnvrionment()

    # -------------------should be defined for each components------------------

    # determine how a class compute hash
    def __hashables__(self):
        raise NotImplementedError("hashables not implemented!")

    def _calculate_component_counter(self) -> Counter:
        raise NotImplementedError("component counter not implemented!")

    def _calculate_all_component(self) -> Set[SQLComponent]:
        raise NotImplementedError("component counter not implemented!")

    def concretize(self, concretize_environment: ConcretizeEnvrionment = None) -> Tuple[
        SQLComponent, ConcretizeEnvrionment]:
        raise NotImplementedError("concretize not implemented!")

    def __str__(self):
        raise NotImplementedError("str not implemented")

    def __repr__(self):
        return self.__str__()

    # -------------------shared properties------------------

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False
        return hash(self) == hash(other)

    def __hash__(self):
        if self._cache.hash is not None:
            return self._cache.hash
        else:
            self._cache.hash = self._calculate_hash()
            return self._cache.hash

    @property
    def cost(self):
        if self._cache.cost is not None:
            return self._cache.cost
        else:
            self._cache.cost = self._calculate_cost()
            return self._cache.cost

    @property
    def component_counter(self) -> Counter:
        if self._cache.component_count is not None:
            return self._cache.component_count
        else:
            self._cache.component_count = self._calculate_component_counter()
            return self._cache.component_count

    @property
    def all_components(self) -> Set[SQLComponent]:
        if self._cache.all_component is not None:
            return self._cache.all_component
        else:
            self._cache.all_component = self._calculate_all_component()
            return self._cache.all_component

    @property
    def provided_chain(self):
        return self._analysis_info.provided_chains

    @property
    def required_chain(self):
        return self._analysis_info.required_chains

    @property
    def data_type(self) -> DataType:
        return self._analysis_info.data_type

    @property
    def depth(self) -> int:
        return self._analysis_info.depth

    def _calculate_hash(self):
        hash_string = self.__class__.__name__ + "#".join([str(hash(getattr(self, i))) for i in self.__hashables__()])
        return hash(hash_string)

    def _calculate_cost(self):
        return CostEstimater.get_cost(self, enforce_recalculate=True)


class NonTerminals(SQLComponent):

    @classmethod
    def generate_from_bank(cls, bank: Bank, cost: int, cost_model: Counter, time_controller: TimeoutController) \
            -> List[SQLComponent]:
        raise NotImplementedError("generate from bank not implemented!")


class Terminals(SQLComponent):
    pass


class SelectClauseItem(SQLComponent):
    pass


class SelectClauseItemWithAlias(SQLComponent):
    __slots__ = ["item", "alias"]

    def __init__(self, item: SelectClauseItem, exact_alias=None):
        self.item = item
        analysis_info = ComponentAnalysisInfo(provided_chains=item.provided_chain,
                                              required_chains=item.required_chain,
                                              data_type=item.data_type)
        if exact_alias is None:
            if isinstance(item, RealCol):
                self.alias = UniqueNameGeneratorForConcretize.generate_col_name(item.col_name)
            elif isinstance(item, AggregationClause):
                self.alias = UniqueNameGeneratorForConcretize.generate_col_name(str(item.func_name))
            else:
                self.alias = UniqueNameGeneratorForConcretize.generate_col_name()
        else:
            self.alias = exact_alias
        super().__init__(analysis_info)

    def get_name(self):
        if self.alias == "":
            item = self.item
            assert isinstance(item, RealCol)
            return item.col_name
        else:
            return self.alias

    def __hashables__(self):
        return ["item", "alias"]

    def _calculate_component_counter(self):
        return Counter({self.item: 1})

    def _calculate_all_component(self) -> Set[SQLComponent]:
        return {self.item}

    def __str__(self):
        if self.alias != "":
            return f'{str(self.item)} AS "{self.alias}"'
        return f"{str(self.item)}"

    def concretize(self, concretize_environment: ConcretizeEnvrionment = None) -> Tuple[
        SQLComponent, ConcretizeEnvrionment]:
        concretized_item, _ = self.item.concretize(concretize_environment)
        return SelectClauseItemWithAlias(concretized_item, exact_alias=self.alias), concretize_environment


class TableClause(Terminals):
    __slots__ = ["table"]

    def __init__(self, table: Table):
        self.table: Table = table
        analysis_info = ComponentAnalysisInfo(provided_chains=tuple([ColChain(table, i[0], i[1])
                                                                     for i in self.table.table_cols]),
                                              required_chains=())
        super().__init__(analysis_info)

    # -------------------should be defined for each components------------------

    # determine how a class compute hash
    def __hashables__(self):
        return ["table"]

    def _calculate_component_counter(self):
        return Counter({self: 1})

    def _calculate_all_component(self) -> Set[SQLComponent]:
        return {self}

    def concretize(self, concretize_environment: ConcretizeEnvrionment = None) -> Tuple[
        SQLComponent, ConcretizeEnvrionment]:
        result = []
        new_concretize_environment = ConcretizeEnvrionment()
        for c in self.table.table_cols:
            new_concretize_environment.add_link(ColChain(self.table, c[0], c[1]),
                                                AliasDicKey(self.table.table_name, c[0]))
        return (self, new_concretize_environment)

    def __str__(self):
        return f'"{self.table.table_name}"'


class GroupByClause(SQLComponent):
    pass


class OrderByClause(SQLComponent):
    pass


class OrderByClauseItem(NonTerminals):
    __slots__ = ["order_by_chain", "order_by_configuration"]

    def __init__(self, order_by_chain: ReferencableChain, order_by_configuration: OrderByConfiguration):
        # deal with property assignments
        self.order_by_chain = order_by_chain
        self.order_by_configuration = order_by_configuration

        analysis_info = ComponentAnalysisInfo(provided_chains=(),
                                              required_chains=(order_by_chain,),
                                              data_type=self.order_by_chain.data_type)
        super().__init__(analysis_info)

    # -------------------should be defined for each components------------------
    @classmethod
    def generate_from_bank(cls, bank: Bank, cost: int, cost_model: Counter, time_controller: TimeoutController) \
            -> List[SQLComponent]:
        results = []
        order_by_chain_candidates = bank.get_from_bank(ReferencableChain, cost, cost_model)
        for order_by_chain_candidate in order_by_chain_candidates:
            new_cost_limit = cost - order_by_chain_candidate.cost
            new_cost_model = cost_model - order_by_chain_candidate.component_counter
            order_by_configuration_candidates = bank.get_from_bank(OrderByConfiguration, new_cost_limit, new_cost_model)
            for order_by_configuration_candidate in order_by_configuration_candidates:
                assert isinstance(order_by_chain_candidate, ReferencableChain)
                assert isinstance(order_by_configuration_candidate, OrderByConfiguration)
                candidate = OrderByClauseItem(order_by_chain_candidate, order_by_configuration_candidate)
                if candidate.cost <= cost and candidate not in bank:
                    results.append(candidate)
        return results

    # determine how a class compute hash
    def __hashables__(self):
        return ["order_by_chain", "order_by_configuration"]

    def _calculate_component_counter(self):
        return self.order_by_chain.component_counter + self.order_by_configuration.component_counter + Counter(
            {self: 1})

    def _calculate_all_component(self) -> Set[SQLComponent]:
        return self.order_by_chain.all_components | self.order_by_configuration.all_components | {self}

    def concretize(self, concretize_environment: ConcretizeEnvrionment = None) -> Tuple[
        SQLComponent, ConcretizeEnvrionment]:
        col, _ = self.order_by_chain.concretize(concretize_environment)
        return OrderByClauseItem(col, self.order_by_configuration), concretize_environment

    def __str__(self):
        return f"{str(self.order_by_chain)} {str(self.order_by_configuration)}"


class OrderByConfiguration(SQLComponent):
    pass


class OrderByConfigurationDESC(OrderByConfiguration, Terminals):
    __slots__ = []

    def __init__(self):
        analysis_info = ComponentAnalysisInfo(provided_chains=tuple(),
                                              required_chains=tuple())
        super().__init__(analysis_info)

    # -------------------should be defined for each components------------------

    # determine how a class compute hash
    def __hashables__(self):
        return []

    def _calculate_component_counter(self):
        return Counter({self: 1})

    def _calculate_all_component(self) -> Set[SQLComponent]:
        return {self}

    def concretize(self, concretize_environment: ConcretizeEnvrionment = None) -> Tuple[
        SQLComponent, ConcretizeEnvrionment]:
        raise NotImplementedError("concretize not implemented!")

    def __str__(self):
        return "DESC"


class OrderByConfigurationASC(OrderByConfiguration, Terminals):
    __slots__ = []

    def __init__(self):
        analysis_info = ComponentAnalysisInfo(provided_chains=tuple(),
                                              required_chains=tuple())
        super().__init__(analysis_info)

    # -------------------should be defined for each components------------------
    # determine how a class compute hash
    def __hashables__(self):
        return []

    def _calculate_component_counter(self):
        return Counter({self: 1})

    def _calculate_all_component(self) -> Set[SQLComponent]:
        return {self}

    def concretize(self, concretize_environment: ConcretizeEnvrionment = None) -> Tuple[
        SQLComponent, ConcretizeEnvrionment]:
        raise NotImplementedError("concretize not implemented!")

    def __str__(self):
        return "ASC"


class SourceClause(SQLComponent):
    pass


class JoinWithOnClause(SourceClause, NonTerminals):
    __slots__ = ["larg", "rarg", "join_type", "on_clause"]

    def __init__(self, larg: SourceClause, rarg: SourceClause, jointype: JoinTypeClause, on_clause: PredicateClause):
        # deal with property assignments
        self.larg = larg
        self.rarg = rarg
        self.join_type = jointype
        self.on_clause = on_clause

        analysis_info = ComponentAnalysisInfo(provided_chains=self.larg.provided_chain + self.rarg.provided_chain,
                                              required_chains=tuple(), depth=max(self.larg.depth, self.rarg.depth))
        super().__init__(analysis_info)

    # -------------------should be defined for each components------------------
    @classmethod
    def generate_from_bank(cls, bank: Bank, cost: int, cost_model: Counter, time_controller: TimeoutController) \
            -> List[SQLComponent]:
        result = []
        source_candidates_1 = bank.get_from_bank(SourceClause, cost, cost_model,
                                                 ConstraintEnvrionment(
                                                     [MaxDepthConstraint(lib.consts.depth_limit - 1)]))
        for source_candidate_1 in source_candidates_1:
            assert isinstance(source_candidate_1, SourceClause)
            new_cost_limit_after_1 = cost - source_candidate_1.cost
            new_cost_model_after_1 = cost_model - source_candidate_1.component_counter
            constraints = [NotInConstraint([source_candidate_1]), MaxDepthConstraint(lib.consts.depth_limit - 1)]
            source_candidates_2 = bank.get_from_bank(SourceClause,
                                                     new_cost_limit_after_1,
                                                     new_cost_model_after_1,
                                                     ConstraintEnvrionment(constraints))
            for source_candidate_2 in source_candidates_2:
                assert isinstance(source_candidate_2, SourceClause)
                new_cost_limit_after_2 = cost - source_candidate_1.cost - source_candidate_2.cost
                new_cost_model_after_2 = cost_model - source_candidate_1.component_counter - \
                                         source_candidate_2.component_counter
                join_types = bank.get_from_bank(JoinTypeClause, new_cost_limit_after_2, new_cost_model_after_2)
                for join_type in join_types:
                    assert isinstance(join_type, JoinTypeClause)
                    new_cost_limit_after_type = cost - source_candidate_1.cost - source_candidate_2.cost - join_type.cost
                    new_cost_model_after_type = cost_model - source_candidate_1.component_counter \
                                                - source_candidate_2.component_counter - join_type.component_counter
                    constraint = [NotNoneConstraint(),
                                  FulfilledByProvidedChainsConstraint(source_candidate_1.provided_chain \
                                                                      + source_candidate_2.provided_chain),
                                  PredicateTwoSideDifferentSourceConstraint(source_candidate_1.provided_chain,
                                                                            source_candidate_2.provided_chain),
                                  NoAggClauseInPredicateConstraint()
                                  ]
                    join_on_clauses = bank.get_from_bank(PredicateClause, new_cost_limit_after_type,
                                                         new_cost_model_after_type,
                                                         ConstraintEnvrionment(constraint))
                    for join_on_clause_candidate in join_on_clauses:
                        assert isinstance(join_on_clause_candidate, PredicateClause)
                        candidate = JoinWithOnClause(source_candidate_1,
                                                     source_candidate_2,
                                                     join_type,
                                                     join_on_clause_candidate)
                        if candidate.cost <= cost and candidate not in bank:
                            result.append(candidate)
        return result

    # determine how a class compute hash
    def __hashables__(self):
        return ["larg", "rarg", "join_type", "on_clause"]

    def _calculate_component_counter(self):
        return self.larg.component_counter + self.rarg.component_counter + self.join_type.component_counter \
               + self.on_clause.component_counter

    def _calculate_all_component(self) -> Set[SQLComponent]:
        return self.larg.all_components | self.rarg.all_components | self.join_type.all_components | \
               self.on_clause.all_components | {self}

    def concretize(self, concretize_environment: ConcretizeEnvrionment = None) -> Tuple[
        SQLComponent, ConcretizeEnvrionment]:
        concretized_larg, env_l = self.larg.concretize(concretize_environment)
        concretized_rarg, env_r = self.rarg.concretize(concretize_environment)
        new_env = env_l.merge(env_r)
        concretized_on, _ = self.on_clause.concretize(new_env)
        return JoinWithOnClause(concretized_larg, concretized_rarg, self.join_type, concretized_on), new_env

    def __str__(self):
        return f"{str(self.larg)} {str(self.join_type)} {str(self.rarg)} ON {str(self.on_clause)}"


class JoinTypeClause(SQLComponent):
    pass


class JoinTypeFull(JoinTypeClause, Terminals):
    __slots__ = []

    def __init__(self):
        analysis_info = ComponentAnalysisInfo(provided_chains=tuple(),
                                              required_chains=tuple())
        super().__init__(analysis_info)

    # -------------------should be defined for each components------------------

    # determine how a class compute hash
    def __hashables__(self):
        return []

    def _calculate_component_counter(self):
        return Counter({self: 1})

    def _calculate_all_component(self) -> Set[SQLComponent]:
        return {self}

    def concretize(self, concretize_environment: ConcretizeEnvrionment = None) -> Tuple[
        SQLComponent, ConcretizeEnvrionment]:
        raise NotImplementedError("concretize not implemented!")

    def __str__(self):
        return "FULL JOIN"

class JoinTypeLeft(JoinTypeClause, Terminals):
    __slots__ = []

    def __init__(self):
        analysis_info = ComponentAnalysisInfo(provided_chains=tuple(),
                                              required_chains=tuple())
        super().__init__(analysis_info)

    # -------------------should be defined for each components------------------

    # determine how a class compute hash
    def __hashables__(self):
        return []

    def _calculate_component_counter(self):
        return Counter({self: 1})

    def _calculate_all_component(self) -> Set[SQLComponent]:
        return {self}

    def concretize(self, concretize_environment: ConcretizeEnvrionment = None) -> Tuple[
        SQLComponent, ConcretizeEnvrionment]:
        raise NotImplementedError("concretize not implemented!")

    def __str__(self):
        return "LEFT JOIN"


class JoinTypeInner(JoinTypeClause, Terminals):
    __slots__ = []

    def __init__(self):
        analysis_info = ComponentAnalysisInfo(provided_chains=tuple(),
                                              required_chains=tuple())
        super().__init__(analysis_info)

    # -------------------should be defined for each components------------------
    @classmethod
    # determine how a class compute hash
    def __hashables__(self):
        return []

    def _calculate_component_counter(self):
        return Counter({self: 1})

    def _calculate_all_component(self) -> Set[SQLComponent]:
        return {self}

    def concretize(self, concretize_environment: ConcretizeEnvrionment = None) -> Tuple[
        SQLComponent, ConcretizeEnvrionment]:
        raise NotImplementedError("concretize not implemented!")

    def __str__(self):
        return "INNER JOIN"


class PredicateClause(SQLComponent):
    pass


class PredicateOpClause(PredicateClause, NonTerminals):
    __slots__ = ["larg", "op", "rarg"]

    def __init__(self, larg: SingleValueClause, op: CompareOp, rarg: SingleValueClause):
        self.larg: SingleValueClause = larg
        self.op: CompareOp = op
        self.rarg: SingleValueClause = rarg

        analysis_info = ComponentAnalysisInfo(provided_chains=tuple(),
                                              required_chains=tuple(set(larg.required_chain + rarg.required_chain)))
        super().__init__(analysis_info)

    # -------------------should be defined for each components------------------
    @classmethod
    def generate_from_bank(cls, bank: Bank, cost: int, cost_model: Counter, time_controller: TimeoutController) \
            -> List[SQLComponent]:
        result = []
        op_candidates = bank.get_from_bank(CompareOp, cost, cost_model)
        for op_candidate in op_candidates:
            assert isinstance(op_candidate, CompareOp)
            new_cost_limit_after_op = cost - op_candidate.cost
            new_cost_model_after_op = cost_model - op_candidate.component_counter
            if isinstance(op_candidate, OpEq) or isinstance(op_candidate, OpNeq):
                constraints_1 = [NotNoneConstraint(),
                                 DataTypeInConstraint([DataType.Number, DataType.Str])]
            else:
                constraints_1 = [NotNoneConstraint(), DataTypeInConstraint([DataType.Number])]

            larg_candidates = bank.get_from_bank(SingleValueClause,
                                                 new_cost_limit_after_op,
                                                 new_cost_model_after_op,
                                                 ConstraintEnvrionment(constraints_1))
            for larg_candidate in larg_candidates:
                assert isinstance(larg_candidate, SingleValueClause)
                new_cost_limit_after_larg = cost - op_candidate.cost - larg_candidate.cost
                new_cost_model_after_larg = cost_model - op_candidate.component_counter - \
                                            larg_candidate.component_counter
                constraints_2 = [NotNoneConstraint(), DataTypeInConstraint([larg_candidate.data_type]),
                                 NotInConstraint([larg_candidate])]
                rarg_candidates = bank.get_from_bank(SingleValueClause, new_cost_limit_after_larg,
                                                     new_cost_model_after_larg,
                                                     ConstraintEnvrionment(constraints_2))
                for rarg_candidate in rarg_candidates:
                    assert isinstance(rarg_candidate, SingleValueClause)
                    candidate = PredicateOpClause(larg_candidate, op_candidate, rarg_candidate)
                    if candidate.cost <= cost and candidate not in bank:
                        result.append(candidate)
        return result

    # determine how a class compute hash
    def __hashables__(self):
        return ["larg", "op", "rarg"]

    def _calculate_component_counter(self):
        # return self.larg.component_counter + self.op.component_counter + self.rarg.component_counter \
        #        + Counter({self: 1})
        return self.larg.component_counter + self.op.component_counter + self.rarg.component_counter \
               + Counter({to_chain(self): 1})

    def _calculate_all_component(self) -> Set[SQLComponent]:
        return self.larg.all_components | self.op.all_components | self.rarg.all_components | {self} | {to_chain(self)}

    def concretize(self, concretize_environment: ConcretizeEnvrionment = None) -> Tuple[
        SQLComponent, ConcretizeEnvrionment]:
        concretized_larg, _ = self.larg.concretize(concretize_environment)
        concretized_rarg, _ = self.rarg.concretize(concretize_environment)
        return PredicateOpClause(concretized_larg, self.op, concretized_rarg), concretize_environment

    def __str__(self):
        return f"{str(self.larg)} {str(self.op)} {str(self.rarg)}"


class PredicateBinOpLogicClause(PredicateClause, NonTerminals):
    __slots__ = ["larg", "op", "rarg"]

    def __init__(self, larg: PredicateClause, op: BinLogicOp, rarg: PredicateClause):
        self.larg: PredicateClause = larg
        self.op: BinLogicOp = op
        self.rarg: PredicateClause = rarg
        analysis_info = ComponentAnalysisInfo(provided_chains=tuple(),
                                              required_chains=tuple(set(self.larg.required_chain +
                                                                        self.rarg.required_chain)))
        super().__init__(analysis_info)

    # -------------------should be defined for each components------------------
    @classmethod
    def generate_from_bank(cls, bank: Bank, cost: int, cost_model: Counter, time_controller: TimeoutController) \
            -> List[SQLComponent]:
        result = []
        op_candidates = bank.get_from_bank(BinLogicOp, cost, cost_model)
        for op_candidate in op_candidates:
            assert isinstance(op_candidate, BinLogicOp)
            new_cost_limit_after_op = cost - op_candidate.cost
            new_cost_model_after_op = cost_model - op_candidate.component_counter

            larg_candidates = bank.get_from_bank(PredicateClause,
                                                 new_cost_limit_after_op,
                                                 new_cost_model_after_op,
                                                 constraint=ConstraintEnvrionment([NotNoneConstraint()]))
            for larg_candidate in larg_candidates:
                assert isinstance(larg_candidate, PredicateClause)
                new_cost_limit_after_larg = cost - op_candidate.cost - larg_candidate.cost
                new_cost_model_after_larg = cost_model - op_candidate.component_counter - \
                                            larg_candidate.component_counter
                constraints = [NotNoneConstraint(), LogicExpressionNoDuplicates(larg_candidate)]
                rarg_candidates = bank.get_from_bank(PredicateClause, new_cost_limit_after_larg,
                                                     new_cost_model_after_larg, ConstraintEnvrionment(constraints))
                for rarg_candidate in rarg_candidates:
                    assert isinstance(rarg_candidate, PredicateClause)
                    candidate = PredicateBinOpLogicClause(larg_candidate, op_candidate, rarg_candidate)
                    if candidate.cost <= cost and candidate not in bank:
                        result.append(candidate)
        return result

    # determine how a class compute hash
    def __hashables__(self):
        return ["larg", "op", "rarg"]

    def _calculate_component_counter(self):
        if isinstance(self.op, OpAnd):
            return self.larg.component_counter + self.op.component_counter + self.rarg.component_counter
        else:
            return self.larg.component_counter + self.op.component_counter + self.rarg.component_counter + Counter(
                {to_chain(self): 1})

    def _calculate_all_component(self) -> Set[SQLComponent]:
        return self.larg.all_components | self.op.all_components | self.rarg.all_components | {self} | {to_chain(self)}

    def concretize(self, concretize_environment: ConcretizeEnvrionment = None) -> Tuple[
        SQLComponent, ConcretizeEnvrionment]:
        concretized_larg, _ = self.larg.concretize(concretize_environment)
        concretized_rarg, _ = self.rarg.concretize(concretize_environment)
        return PredicateBinOpLogicClause(concretized_larg, self.op, concretized_rarg), concretize_environment

    def __str__(self):
        return f"{str(self.larg)} {str(self.op)} {str(self.rarg)}"


class SingleValueClause(SelectClauseItem):
    pass


class WindowFuncCallClause(SelectClauseItem):
    pass


class WindowFuncCallClauseAgg(WindowFuncCallClause, NonTerminals):
    __slots__ = ["agg_func", "partition_clause", "orderby_clause"]

    def __init__(self, agg_func: AggregationClause, partition_clause: PartitionClause, order_by_clause: OrderByClause):
        self.agg_func: AggregationClause = agg_func
        self.partition_clause: PartitionClause = partition_clause
        self.orderby_clause: OrderByClause = order_by_clause

        analysis_info = ComponentAnalysisInfo(provided_chains=self.agg_func.provided_chain,
                                              required_chains=tuple(set(self.agg_func.required_chain +
                                                                        self.partition_clause.required_chain +
                                                                        self.orderby_clause.required_chain)),
                                              data_type=DataType.Number)
        super().__init__(analysis_info)

    # -------------------should be defined for each components------------------
    @classmethod
    def generate_from_bank(cls, bank: Bank, cost: int, cost_model: Counter, time_controller: TimeoutController) \
            -> List[SQLComponent]:
        result = []
        agg_func_candidates = bank.get_from_bank(AggregationClause, cost, cost_model)
        for agg_func_candidate in agg_func_candidates:
            assert isinstance(agg_func_candidate, AggregationClause)
            new_cost_limit_after_agg = cost - agg_func_candidate.cost
            new_cost_model_after_agg = cost_model - agg_func_candidate.component_counter

            partition_candidates = bank.get_from_bank(PartitionClause,
                                                      new_cost_limit_after_agg,
                                                      new_cost_model_after_agg,
                                                      )
            for partition_candidate in partition_candidates:
                assert isinstance(partition_candidate, PartitionClause)
                new_cost_limit_after_partition = cost - agg_func_candidate.cost - partition_candidate.cost
                new_cost_model_after_partition = cost_model - agg_func_candidate.component_counter - \
                                                 partition_candidate.component_counter
                order_by_candidates = bank.get_from_bank(OrderByClause, new_cost_limit_after_partition,
                                                         new_cost_model_after_partition)
                for order_by_candidate in order_by_candidates:
                    assert isinstance(order_by_candidate, OrderByClause)
                    if isinstance(order_by_candidate, NoneClause) and isinstance(partition_candidate, NoneClause):
                        continue
                    candidate = WindowFuncCallClauseAgg(agg_func_candidate, partition_candidate, order_by_candidate)
                    if candidate.cost <= cost and candidate not in bank:
                        result.append(candidate)
        return result

    # determine how a class compute hash
    def __hashables__(self):
        return ["agg_func", "partition_clause", "orderby_clause"]

    def _calculate_component_counter(self):
        return self.agg_func.component_counter + self.partition_clause.component_counter \
               + self.orderby_clause.component_counter 

    def _calculate_all_component(self) -> Set[SQLComponent]:
        return self.agg_func.all_components | self.partition_clause.all_components | self.orderby_clause.all_components | {
            self}

    def concretize(self, concretize_environment: ConcretizeEnvrionment = None) -> Tuple[
        SQLComponent, ConcretizeEnvrionment]:
        concretized_agg_func, _ = self.agg_func.concretize(concretize_environment)
        concretized_partition_clause, _ = self.partition_clause.concretize(concretize_environment)
        concretized_order_by_clause, _ = self.orderby_clause.concretize(concretize_environment)
        return WindowFuncCallClauseAgg(concretized_agg_func, concretized_partition_clause, concretized_order_by_clause), \
               concretize_environment

    def __str__(self):
        result = f"{str(self.agg_func)} OVER ("
        if not isinstance(self.partition_clause, NoneClause):
            result += f" PARTITION BY {str(self.partition_clause)}"
        if not isinstance(self.orderby_clause, NoneClause):
            result += f" ORDER BY {str(self.orderby_clause)}"
        result += " )"
        return result


class WindowFuncCallClauseWinFun(WindowFuncCallClause, NonTerminals):
    __slots__ = ["win_func", "partition_clause", "orderby_clause"]

    def __init__(self, win_func: WindowFuncClause, partition_clause: PartitionClause, order_by_clause: OrderByClause):
        self.win_func: WindowFuncClause = win_func
        self.partition_clause: PartitionClause = partition_clause
        self.orderby_clause: OrderByClause = order_by_clause

        analysis_info = ComponentAnalysisInfo(provided_chains=self.win_func.provided_chain,
                                              required_chains=tuple(set(self.win_func.required_chain +
                                                                        self.partition_clause.required_chain +
                                                                        self.orderby_clause.required_chain)),
                                              data_type=DataType.Number)
        super().__init__(analysis_info)

    # -------------------should be defined for each components------------------
    @classmethod
    def generate_from_bank(cls, bank: Bank, cost: int, cost_model: Counter, time_controller: TimeoutController) \
            -> List[SQLComponent]:
        result = []
        win_func_candidates = bank.get_from_bank(WindowFuncClause, cost, cost_model)
        for win_func_candidate in win_func_candidates:
            assert isinstance(win_func_candidate, WindowFuncClause)
            new_cost_limit_after_agg = cost - win_func_candidate.cost
            new_cost_model_after_agg = cost_model - win_func_candidate.component_counter

            partition_candidates = bank.get_from_bank(PartitionClause,
                                                      new_cost_limit_after_agg,
                                                      new_cost_model_after_agg,
                                                      )
            for partition_candidate in partition_candidates:
                assert isinstance(partition_candidate, PartitionClause)
                new_cost_limit_after_partition = cost - win_func_candidate.cost - partition_candidate.cost
                new_cost_model_after_partition = cost_model - win_func_candidate.component_counter - \
                                                 partition_candidate.component_counter
                if isinstance(win_func_candidate.funcname, WindowFuncNameDenseRank):
                    constraints = [NotNoneConstraint()]
                else:
                    constraints = []
                order_by_candidates = bank.get_from_bank(OrderByClause, new_cost_limit_after_partition,
                                                         new_cost_model_after_partition,
                                                         ConstraintEnvrionment(constraints))
                for order_by_candidate in order_by_candidates:
                    assert isinstance(order_by_candidate, OrderByClause)
                    candidate = WindowFuncCallClauseWinFun(win_func_candidate, partition_candidate, order_by_candidate)
                    if candidate.cost <= cost and candidate not in bank:
                        result.append(candidate)
        return result

    # determine how a class compute hash
    def __hashables__(self):
        return ["win_func", "partition_clause", "orderby_clause"]

    def _calculate_component_counter(self):
        return self.win_func.component_counter + self.partition_clause.component_counter \
               + self.orderby_clause.component_counter + Counter({self: 1})

    def _calculate_all_component(self) -> Set[SQLComponent]:
        return self.win_func.all_components | self.partition_clause.all_components | self.orderby_clause.all_components \
               | self.orderby_clause.all_components | {self}

    def concretize(self, concretize_environment: ConcretizeEnvrionment = None) -> Tuple[
        SQLComponent, ConcretizeEnvrionment]:
        concretized_winfuc, _ = self.win_func.concretize(concretize_environment)
        concretized_order_by, _ = self.orderby_clause.concretize(concretize_environment)
        concretized_partition_by, _ = self.partition_clause.concretize(concretize_environment)
        return WindowFuncCallClauseWinFun(concretized_winfuc, concretized_partition_by, concretized_order_by), \
               concretize_environment

    def __str__(self):
        result = f"{str(self.win_func)} OVER ("
        if self.partition_clause is not None:
            result += f" PARTITION BY {str(self.partition_clause)}"
        if self.orderby_clause is not None:
            result += f" ORDER BY {str(self.orderby_clause)}"
        return result


class WindowFuncClause(NonTerminals):
    __slots__ = ["funcname"]

    def __init__(self, funcname: WindowFuncName):
        self.funcname: WindowFuncName = funcname

        analysis_info = ComponentAnalysisInfo(provided_chains=tuple([WindowFuncChain(self.funcname, NoneClause())]),
                                              required_chains=tuple())
        super().__init__(analysis_info)

    # -------------------should be defined for each components------------------
    @classmethod
    def generate_from_bank(cls, bank: Bank, cost: int, cost_model: Counter, time_controller: TimeoutController) \
            -> List[SQLComponent]:
        result = []
        win_func_name_candidates = bank.get_from_bank(WindowFuncName, cost, cost_model)
        for win_func_name_candidate in win_func_name_candidates:
            assert isinstance(win_func_name_candidate, WindowFuncName)
            candidate = WindowFuncClause(win_func_name_candidate)
            if candidate.cost <= cost and candidate not in bank:
                result.append(candidate)
        return result

    # determine how a class compute hash
    def __hashables__(self):
        return ["funcname"]

    def _calculate_component_counter(self):
        return self.funcname.component_counter

    def _calculate_all_component(self) -> Set[SQLComponent]:
        return self.funcname.all_components | {self}

    def concretize(self, concretize_environment: ConcretizeEnvrionment = None) -> Tuple[
        SQLComponent, ConcretizeEnvrionment]:
        return self, concretize_environment

    def __str__(self):
        return f"{str(self.funcname)}()"


class WindowFuncName(SQLComponent):
    pass


class WindowFuncNameDenseRank(WindowFuncName, Terminals):
    __slots__ = []

    def __init__(self):
        analysis_info = ComponentAnalysisInfo(provided_chains=tuple(),
                                              required_chains=tuple())
        super().__init__(analysis_info)

    # -------------------should be defined for each components------------------
    # determine how a class compute hash
    def __hashables__(self):
        return []

    def _calculate_component_counter(self):
        return Counter({self: 1})

    def _calculate_all_component(self) -> Set[SQLComponent]:
        return {self}

    def concretize(self, concretize_environment: ConcretizeEnvrionment = None) -> Tuple[
        SQLComponent, ConcretizeEnvrionment]:
        raise NotImplementedError("concretize not implemented!")

    def __str__(self):
        return "dense_rank"


class AggregationClause(SingleValueClause, NonTerminals):
    __slots__ = ["func_name", "parameter", "distinct_clause"]

    def __init__(self, func_name: AggregationFuncName, parameter: ReferencableChain,
                 distinct_clause: NoneClause | DistinctClause):
        self.func_name: AggregationFuncName = func_name
        self.parameter: ReferencableChain = parameter
        self.distinct_clause: NoneClause | DistinctClause = distinct_clause

        analysis_info = ComponentAnalysisInfo(
            provided_chains=(AggChain(self.func_name, self.parameter.provided_chain[0]),),
            required_chains=(self.parameter.provided_chain[0],),
            data_type=DataType.Number)
        super().__init__(analysis_info)

    # -------------------should be defined for each components------------------
    @classmethod
    def generate_from_bank(cls, bank: Bank, cost: int, cost_model: Counter, time_controller: TimeoutController) \
            -> List[SQLComponent]:
        result = []
        agg_func_name_candidates = bank.get_from_bank(AggregationFuncName, cost, cost_model)
        for agg_func_name_candidate in agg_func_name_candidates:
            assert isinstance(agg_func_name_candidate, AggregationFuncName)
            new_cost_limit_after_agg = cost - agg_func_name_candidate.cost
            new_cost_model_after_agg = cost_model - agg_func_name_candidate.component_counter
            if isinstance(agg_func_name_candidate, AggregationFuncNameCount):
                constraints = []
            else:
                constraints = [DataTypeInConstraint([DataType.Number])]

            parameter_candidates = bank.get_from_bank(ReferencableChain,
                                                      new_cost_limit_after_agg,
                                                      new_cost_model_after_agg,
                                                      ConstraintEnvrionment(constraints)
                                                      )
            for parameter_candidate in parameter_candidates:
                assert isinstance(parameter_candidate, ReferencableChain)
                candidate = AggregationClause(agg_func_name_candidate, parameter_candidate, NoneClause())
                if candidate.cost <= cost and candidate not in bank:
                    result.append(candidate)
                candidate = AggregationClause(agg_func_name_candidate, parameter_candidate, DistinctClause())
                if candidate.cost <= cost and candidate not in bank:
                    result.append(candidate)
        return result

    # determine how a class compute hash
    def __hashables__(self):
        return ["func_name", "parameter", "distinct_clause"]

    def _calculate_component_counter(self):
        # return self.func_name.component_counter + self.parameter.component_counter + Counter(
        #     {self: 1})
        return to_chain(self).component_counter + self.distinct_clause.component_counter

    def _calculate_all_component(self) -> Set[SQLComponent]:
        # return {self} | self.func_name.all_components | self.parameter.all_components
        return {to_chain(self)} | {
            AggregationClause(self.func_name, self.parameter, NoneClause())} | self.distinct_clause.all_components

    def concretize(self, concretize_environment: ConcretizeEnvrionment = None) -> Tuple[
        SQLComponent, ConcretizeEnvrionment]:
        concretized_parameter, _ = self.parameter.concretize(concretize_environment)
        return AggregationClause(self.func_name, concretized_parameter,
                                 distinct_clause=self.distinct_clause), concretize_environment

    def __str__(self):
        if isinstance(self.distinct_clause, NoneClause):
            return f"{str(self.func_name)}({str(self.parameter)})"
        else:
            return f"{str(self.func_name)}(DISTINCT {str(self.parameter)})"


class AggregationFuncName(SQLComponent):
    pass


class AggregationFuncNameCount(AggregationFuncName, Terminals):
    __slots__ = []

    def __init__(self):
        analysis_info = ComponentAnalysisInfo(provided_chains=tuple(),
                                              required_chains=tuple())
        super().__init__(analysis_info)

    # -------------------should be defined for each components------------------

    # determine how a class compute hash
    def __hashables__(self):
        return []

    def _calculate_component_counter(self):
        return Counter({self: 1})

    def _calculate_all_component(self) -> Set[SQLComponent]:
        return {self}

    def concretize(self, concretize_environment: ConcretizeEnvrionment = None) -> Tuple[
        SQLComponent, ConcretizeEnvrionment]:
        raise NotImplementedError("concretize not implemented!")

    def __str__(self):
        return "COUNT"


class AggregationFuncNameSum(AggregationFuncName, Terminals):
    __slots__ = []

    def __init__(self):
        analysis_info = ComponentAnalysisInfo(provided_chains=tuple(),
                                              required_chains=tuple())
        super().__init__(analysis_info)

    # -------------------should be defined for each components------------------

    # determine how a class compute hash
    def __hashables__(self):
        return []

    def _calculate_component_counter(self):
        return Counter({self: 1})

    def _calculate_all_component(self) -> Set[SQLComponent]:
        return {self}

    def concretize(self, concretize_environment: ConcretizeEnvrionment = None) -> Tuple[
        SQLComponent, ConcretizeEnvrionment]:
        raise NotImplementedError("concretize not implemented!")

    def __str__(self):
        return "SUM"


class AggregationFuncNameAvg(AggregationFuncName, Terminals):
    __slots__ = []

    def __init__(self):
        analysis_info = ComponentAnalysisInfo(provided_chains=tuple(),
                                              required_chains=tuple())
        super().__init__(analysis_info)

    # -------------------should be defined for each components------------------

    # determine how a class compute hash
    def __hashables__(self):
        return []

    def _calculate_component_counter(self):
        return Counter({self: 1})

    def _calculate_all_component(self) -> Set[SQLComponent]:
        return {self}

    def concretize(self, concretize_environment: ConcretizeEnvrionment = None) -> Tuple[
        SQLComponent, ConcretizeEnvrionment]:
        raise NotImplementedError("concretize not implemented!")

    def __str__(self):
        return "AVG"


class AggregationFuncNameMax(AggregationFuncName, Terminals):
    __slots__ = []

    def __init__(self):
        analysis_info = ComponentAnalysisInfo(provided_chains=tuple(),
                                              required_chains=tuple())
        super().__init__(analysis_info)

    # -------------------should be defined for each components------------------

    # determine how a class compute hash
    def __hashables__(self):
        return []

    def _calculate_component_counter(self):
        return Counter({self: 1})

    def _calculate_all_component(self) -> Set[SQLComponent]:
        return {self}

    def concretize(self, concretize_environment: ConcretizeEnvrionment = None) -> Tuple[
        SQLComponent, ConcretizeEnvrionment]:
        raise NotImplementedError("concretize not implemented!")

    def __str__(self):
        return "MAX"


class AggregationFuncNameMin(AggregationFuncName, Terminals):
    __slots__ = []

    def __init__(self):
        analysis_info = ComponentAnalysisInfo(provided_chains=tuple(),
                                              required_chains=tuple())
        super().__init__(analysis_info)

    # -------------------should be defined for each components------------------

    # determine how a class compute hash
    def __hashables__(self):
        return []

    def _calculate_component_counter(self):
        return Counter({self: 1})

    def _calculate_all_component(self) -> Set[SQLComponent]:
        return {self}

    def concretize(self, concretize_environment: ConcretizeEnvrionment = None) -> Tuple[
        SQLComponent, ConcretizeEnvrionment]:
        raise NotImplementedError("concretize not implemented!")

    def __str__(self):
        return "MIN"

class AggregationFuncNameBitAnd(AggregationFuncName, Terminals):
    __slots__ = []

    def __init__(self):
        analysis_info = ComponentAnalysisInfo(provided_chains=tuple(),
                                              required_chains=tuple())
        super().__init__(analysis_info)

    # -------------------should be defined for each components------------------

    # determine how a class compute hash
    def __hashables__(self):
        return []

    def _calculate_component_counter(self):
        return Counter({self: 1})

    def _calculate_all_component(self) -> Set[SQLComponent]:
        return {self}

    def concretize(self, concretize_environment: ConcretizeEnvrionment = None) -> Tuple[
        SQLComponent, ConcretizeEnvrionment]:
        raise NotImplementedError("concretize not implemented!")

    def __str__(self):
        return "BIT_AND"

class AggregationFuncNameBitOr(AggregationFuncName, Terminals):
    __slots__ = []

    def __init__(self):
        analysis_info = ComponentAnalysisInfo(provided_chains=tuple(),
                                              required_chains=tuple())
        super().__init__(analysis_info)

    # -------------------should be defined for each components------------------

    # determine how a class compute hash
    def __hashables__(self):
        return []

    def _calculate_component_counter(self):
        return Counter({self: 1})

    def _calculate_all_component(self) -> Set[SQLComponent]:
        return {self}

    def concretize(self, concretize_environment: ConcretizeEnvrionment = None) -> Tuple[
        SQLComponent, ConcretizeEnvrionment]:
        raise NotImplementedError("concretize not implemented!")

    def __str__(self):
        return "BIT_OR"


class PartitionClause(SQLComponent):
    pass


class PartitionClauseList(PartitionClause, NonTerminals):
    __slots__ = ["items"]

    def __init__(self, items: Tuple[ReferencableChain]):
        self.items: Tuple[ReferencableChain] = tuple(sorted(list(items), key=lambda x: hash(x)))

        r_chains = []
        for i in self.items:
            r_chains += i.required_chain
        analysis_info = ComponentAnalysisInfo(provided_chains=tuple(),
                                              required_chains=tuple(set(r_chains)))
        super().__init__(analysis_info)

    # -------------------should be defined for each components------------------
    @classmethod
    def generate_from_bank(cls, bank: Bank, cost: int, cost_model: Counter, time_controller: TimeoutController) \
            -> List[SQLComponent]:
        result = []
        candidate_chains = bank.get_from_bank(ReferencableChain, cost, cost_model)
        candidate_chain_tuple_list = get_all_combinations_below_n(candidate_chains, lib.consts.list_length_limit)
        for candidate_chain_tuple in candidate_chain_tuple_list:
            candidate = PartitionClauseList(candidate_chain_tuple)
            if candidate.cost <= cost and candidate not in bank:
                result.append(candidate)
        return result

    # determine how a class compute hash
    def __hashables__(self):
        return ["items"]

    def _calculate_component_counter(self):
        counter = Counter()
        for i in self.items:
            counter += i.component_counter
        counter += Counter({self: 1})
        return counter

    def _calculate_all_component(self) -> Set[SQLComponent]:
        result = {self}
        for i in self.items:
            result |= i.all_components
        return result

    def concretize(self, concretize_environment: ConcretizeEnvrionment = None) -> Tuple[
        SQLComponent, ConcretizeEnvrionment]:
        concretized_list = []
        for i in self.items:
            concretized_item, _ = i.concretize(concretize_environment)
            concretized_list.append(concretized_item)
        return PartitionClauseList(tuple(concretized_list)), concretize_environment

    def __str__(self):
        strs = []
        for i in self.items:
            strs.append(str(i))
        return ",".join(strs)


class Chain(SingleValueClause):
    def eq_modulo_table(self, other_chain: Chain):
        pass


class ReferencableChain(Chain):
    pass


class NonReferencableChain(Chain):
    pass


class SelectClause(SQLComponent):
    __slots__ = ["items"]

    def __init__(self, items: Tuple[SelectClauseItem, ...]):
        self.items: Tuple[SelectClauseItem, ...] = tuple(list(items))

        provided_chains = []
        required_chains = []
        for i in self.items:
            provided_chains += i.provided_chain
            required_chains += i.required_chain

        analysis_info = ComponentAnalysisInfo(provided_chains=tuple(provided_chains),
                                              required_chains=tuple(set(required_chains)))
        super().__init__(analysis_info)
        # -------------------should be defined for each components------------------

    # determine how a class compute hash
    def __hashables__(self):
        return ["items"]

    def _calculate_component_counter(self):
        counter = Counter()
        for i in self.items:
            counter |= i.component_counter
        return counter

    def _calculate_all_component(self) -> Set[SQLComponent]:
        result = {self}
        for i in self.items:
            result |= i.all_components
        return result

    def concretize(self, concretize_environment: ConcretizeEnvrionment = None) -> Tuple[
        SQLComponent, ConcretizeEnvrionment]:
        new_envrionment = ConcretizeEnvrionment()
        concretized_list = []
        for i in self.items:
            concretized_item, _ = i.concretize(concretize_environment)
            concretized_list.append(concretized_item)
        wrapped_with_alias_list: List[SelectClauseItemWithAlias] = []
        for i in concretized_list:
            if not isinstance(i, SelectClauseItemWithAlias):
                wrapped_with_alias_list.append(SelectClauseItemWithAlias(i))
            else:
                wrapped_with_alias_list.append(i)
        for i in wrapped_with_alias_list:
            new_envrionment.add_link(i.provided_chain[0], AliasDicKey("", i.get_name()))
        return SelectClause(tuple(wrapped_with_alias_list)), new_envrionment

    def __str__(self):
        strs = []
        for i in self.items:
            strs.append(str(i))
        return ", ".join(strs)


class DistinctClause(SQLComponent):
    def __init__(self):
        analysis_info = ComponentAnalysisInfo(provided_chains=tuple(),
                                              required_chains=tuple())
        super().__init__(analysis_info)

    # -------------------should be defined for each components------------------

    # determine how a class compute hash
    def __hashables__(self):
        return []

    def _calculate_component_counter(self):
        return Counter({self: 1})

    def _calculate_all_component(self) -> Set[SQLComponent]:
        return {self}

    def concretize(self, concretize_environment: ConcretizeEnvrionment = None) -> Tuple[
        SQLComponent, ConcretizeEnvrionment]:
        return (self, concretize_environment)

    def __str__(self):
        return "DISTINCT"


class StringConst(NonReferencableChain, Terminals):
    __slots__ = ["val"]

    def __init__(self, val: str):
        # deal with property assignments
        self.val: str = val

        analysis_info = ComponentAnalysisInfo(provided_chains=(self,),
                                              required_chains=tuple(),
                                              data_type=DataType.Str)
        super().__init__(analysis_info)

    # -------------------should be defined for each components------------------

    # determine how a class compute hash
    def __hashables__(self):
        return ["val"]

    def _calculate_component_counter(self):
        return Counter({self: 1})

    def _calculate_all_component(self) -> Set[SQLComponent]:
        return {self}

    def concretize(self, concretize_environment: ConcretizeEnvrionment = None) -> Tuple[
        SQLComponent, ConcretizeEnvrionment]:
        return self, concretize_environment

    def __str__(self):
        return f"'{self.val}'"


class AliasStringConst(NonReferencableChain, Terminals):
    __slots__ = ["val"]

    def __init__(self, val: str):
        # deal with property assignments
        self.val: str = val

        analysis_info = ComponentAnalysisInfo(provided_chains=tuple(),
                                              required_chains=tuple())
        super().__init__(analysis_info)

    # -------------------should be defined for each components------------------

    # determine how a class compute hash
    def __hashables__(self):
        return ["val"]

    def _calculate_component_counter(self):
        return Counter()

    def _calculate_all_component(self) -> Set[SQLComponent]:
        return set()

    def concretize(self, concretize_environment: ConcretizeEnvrionment = None) -> Tuple[
        SQLComponent, ConcretizeEnvrionment]:
        raise NotImplementedError("concretize not implemented!")

    def __str__(self):
        return f'{self.val}'


class IntConst(NonReferencableChain, Terminals):
    __slots__ = ["val"]

    def __init__(self, val: int):
        # deal with property assignments
        self.val: int = val

        analysis_info = ComponentAnalysisInfo(provided_chains=(self,),
                                              required_chains=tuple(),
                                              data_type=DataType.Number)
        super().__init__(analysis_info)

    # -------------------should be defined for each components------------------

    # determine how a class compute hash
    def __hashables__(self):
        return ["val"]

    def _calculate_component_counter(self):
        return Counter({self: 1})

    def _calculate_all_component(self) -> Set[SQLComponent]:
        return {self}

    def concretize(self, concretize_environment: ConcretizeEnvrionment = None) -> Tuple[
        SQLComponent, ConcretizeEnvrionment]:
        return self, concretize_environment

    def __str__(self):
        return str(self.val)


class FloatConst(NonReferencableChain, Terminals):
    __slots__ = ["val"]

    def __init__(self, val: float):
        # deal with property assignments
        self.val: float = val

        analysis_info = ComponentAnalysisInfo(provided_chains=(self,),
                                              required_chains=tuple(),
                                              data_type=DataType.Number)
        super().__init__(analysis_info)

    # -------------------should be defined for each components------------------

    # determine how a class compute hash
    def __hashables__(self):
        return ["val"]

    def _calculate_component_counter(self):
        return Counter({self: 1})

    def _calculate_all_component(self) -> Set[SQLComponent]:
        return {self}

    def concretize(self, concretize_environment: ConcretizeEnvrionment = None) -> Tuple[
        SQLComponent, ConcretizeEnvrionment]:
        return self, concretize_environment

    def __str__(self):
        return str(self.val)


class CompareOp(SQLComponent):
    pass


class OpLt(CompareOp, Terminals):
    __slots__ = []

    def __init__(self):
        analysis_info = ComponentAnalysisInfo(provided_chains=tuple(),
                                              required_chains=tuple())
        super().__init__(analysis_info)

    # -------------------should be defined for each components------------------

    # determine how a class compute hash
    def __hashables__(self):
        return []

    def _calculate_component_counter(self):
        return Counter({self: 1})

    def _calculate_all_component(self) -> Set[SQLComponent]:
        return {self}

    def concretize(self, concretize_environment: ConcretizeEnvrionment = None) -> Tuple[
        SQLComponent, ConcretizeEnvrionment]:
        raise NotImplementedError("concretize not implemented!")

    def __str__(self):
        return "<"


class OpLeq(CompareOp, Terminals):
    __slots__ = []

    def __init__(self):
        analysis_info = ComponentAnalysisInfo(provided_chains=tuple(),
                                              required_chains=tuple())
        super().__init__(analysis_info)

    # -------------------should be defined for each components------------------

    # determine how a class compute hash
    def __hashables__(self):
        return []

    def _calculate_component_counter(self):
        return Counter({self: 1})

    def _calculate_all_component(self) -> Set[SQLComponent]:
        return {self}

    def concretize(self, concretize_environment: ConcretizeEnvrionment = None) -> Tuple[
        SQLComponent, ConcretizeEnvrionment]:
        raise NotImplementedError("concretize not implemented!")

    def __str__(self):
        return "<="


class OpEq(CompareOp, Terminals):
    __slots__ = []

    def __init__(self):
        analysis_info = ComponentAnalysisInfo(provided_chains=tuple(),
                                              required_chains=tuple())
        super().__init__(analysis_info)

    # -------------------should be defined for each components------------------

    # determine how a class compute hash
    def __hashables__(self):
        return []

    def _calculate_component_counter(self):
        return Counter({self: 1})

    def _calculate_all_component(self) -> Set[SQLComponent]:
        return {self}

    def concretize(self, concretize_environment: ConcretizeEnvrionment = None) -> Tuple[
        SQLComponent, ConcretizeEnvrionment]:
        raise NotImplementedError("concretize not implemented!")

    def __str__(self):
        return "="


class OpNeq(CompareOp, Terminals):
    __slots__ = []

    def __init__(self):
        analysis_info = ComponentAnalysisInfo(provided_chains=tuple(),
                                              required_chains=tuple())
        super().__init__(analysis_info)

    # -------------------should be defined for each components------------------

    # determine how a class compute hash
    def __hashables__(self):
        return []

    def _calculate_component_counter(self):
        return Counter({self: 1})

    def _calculate_all_component(self) -> Set[SQLComponent]:
        return {self}

    def concretize(self, concretize_environment: ConcretizeEnvrionment = None) -> Tuple[
        SQLComponent, ConcretizeEnvrionment]:
        raise NotImplementedError("concretize not implemented!")

    def __str__(self):
        return "!="


class BinLogicOp(SQLComponent):
    pass


class OpAnd(BinLogicOp, Terminals):
    __slots__ = []

    def __init__(self):
        analysis_info = ComponentAnalysisInfo(provided_chains=tuple(),
                                              required_chains=tuple())
        super().__init__(analysis_info)

    # -------------------should be defined for each components------------------

    # determine how a class compute hash
    def __hashables__(self):
        return []

    def _calculate_component_counter(self):
        return Counter({self: 1})

    def _calculate_all_component(self) -> Set[SQLComponent]:
        return {self}

    def concretize(self, concretize_environment: ConcretizeEnvrionment = None) -> Tuple[
        SQLComponent, ConcretizeEnvrionment]:
        raise NotImplementedError("concretize not implemented!")

    def __str__(self):
        return "AND"


class OpOr(BinLogicOp, Terminals):
    __slots__ = []

    def __init__(self):
        analysis_info = ComponentAnalysisInfo(provided_chains=tuple(),
                                              required_chains=tuple())
        super().__init__(analysis_info)

    # -------------------should be defined for each components------------------

    # determine how a class compute hash
    def __hashables__(self):
        return []

    def _calculate_component_counter(self):
        return Counter({self: 1})

    def _calculate_all_component(self) -> Set[SQLComponent]:
        return {self}

    def concretize(self, concretize_environment: ConcretizeEnvrionment = None) -> Tuple[
        SQLComponent, ConcretizeEnvrionment]:
        raise NotImplementedError("concretize not implemented!")

    def __str__(self):
        return "OR"


class GroupByClauseList(GroupByClause, NonTerminals):
    __slots__ = ["group_list"]

    def __init__(self, group_list: Tuple[ReferencableChain]):
        # deal with property assignments
        self.group_list: Tuple[ReferencableChain] = tuple(sorted(list(group_list), key=lambda x: hash(x)))

        required_chains = []
        for i in self.group_list:
            required_chains += i.required_chain
        analysis_info = ComponentAnalysisInfo(provided_chains=tuple(),
                                              required_chains=tuple(set(required_chains)))
        super().__init__(analysis_info)

    # -------------------should be defined for each components------------------
    @classmethod
    def generate_from_bank(cls, bank: Bank, cost: int, cost_model: Counter, time_controller: TimeoutController) \
            -> List[SQLComponent]:
        result = []
        candidate_chains = bank.get_from_bank(ReferencableChain, cost, cost_model)
        candidate_chain_tuple_list = get_all_combinations_below_n(candidate_chains, lib.consts.list_length_limit)
        for candidate_chain_tuple in candidate_chain_tuple_list:
            candidate = GroupByClauseList(candidate_chain_tuple)
            if candidate.cost <= cost and candidate not in bank:
                result.append(candidate)
        return result

    # determine how a class compute hash
    def __hashables__(self):
        return ["group_list"]

    def _calculate_component_counter(self):
        counter = Counter()
        for i in self.group_list:
            counter += i.component_counter
        counter += Counter({self: 1})
        return counter

    def _calculate_all_component(self) -> Set[SQLComponent]:
        result = {self}
        for i in self.group_list:
            result |= i.all_components
        return result

    def concretize(self, concretize_environment: ConcretizeEnvrionment = None) -> Tuple[
        SQLComponent, ConcretizeEnvrionment]:
        concretized_list = []
        for i in self.group_list:
            concretized_item, _ = i.concretize(concretize_environment)
            concretized_list.append(concretized_item)
        return PartitionClauseList(tuple(concretized_list)), concretize_environment

    def __str__(self):
        strs = []
        for i in self.group_list:
            strs.append(str(i))
        return ",".join(strs)


class OrderByClauseList(OrderByClause, NonTerminals):
    __slots__ = ["order_list"]

    def __init__(self, order_list: Tuple[OrderByClauseItem]):
        # deal with property assignments
        self.order_list: Tuple[OrderByClauseItem] = tuple(sorted(list(order_list), key=lambda x: hash(x)))

        required_chains = []
        for i in self.order_list:
            required_chains += i.required_chain

        analysis_info = ComponentAnalysisInfo(provided_chains=tuple(),
                                              required_chains=tuple(set(required_chains)))
        super().__init__(analysis_info)

    # -------------------should be defined for each components------------------
    @classmethod
    def generate_from_bank(cls, bank: Bank, cost: int, cost_model: Counter, time_controller: TimeoutController) \
            -> List[SQLComponent]:
        result = []
        candidate_items = bank.get_from_bank(OrderByClauseItem, cost, cost_model)
        candidate_item_tuple_list = get_all_combinations_below_n(candidate_items, lib.consts.list_length_limit)
        for candidate_item_tuple in candidate_item_tuple_list:
            candidate = OrderByClauseList(candidate_item_tuple)
            if candidate.cost <= cost and candidate not in bank:
                result.append(candidate)
        return result

    # determine how a class compute hash
    def __hashables__(self):
        return ["order_list"]

    def _calculate_component_counter(self):
        counter = Counter()
        for i in self.order_list:
            counter += i.component_counter
        counter += Counter({self: 1})
        return counter

    def _calculate_all_component(self) -> Set[SQLComponent]:
        result = {self}
        for i in self.order_list:
            result |= i.all_components
        return result

    def concretize(self, concretize_environment: ConcretizeEnvrionment = None) -> Tuple[
        SQLComponent, ConcretizeEnvrionment]:
        concretized_list = []
        for i in self.order_list:
            concretized_item, _ = i.concretize(concretize_environment)
            concretized_list.append(concretized_item)
        return PartitionClauseList(tuple(concretized_list)), concretize_environment

    def __str__(self):
        strs = []
        for i in self.order_list:
            strs.append(str(i))
        return ",".join(strs)


class NoneClause(GroupByClause, OrderByClause, PredicateClause, PartitionClause,
                 NonReferencableChain, Terminals):
    __slots__ = []

    def __init__(self):
        analysis_info = ComponentAnalysisInfo(provided_chains=tuple(),
                                              required_chains=tuple())
        super().__init__(analysis_info)

    # -------------------should be defined for each components------------------

    # determine how a class compute hash
    def __hashables__(self):
        return []

    def _calculate_component_counter(self):
        return Counter()

    def _calculate_all_component(self) -> Set[SQLComponent]:
        return {self}

    def concretize(self, concretize_environment: ConcretizeEnvrionment = None) -> Tuple[
        SQLComponent, ConcretizeEnvrionment]:
        return self, concretize_environment

    def __str__(self):
        return "<NoneClause>"


class QueryComponentSource(SourceClause, NonTerminals):
    __slots__ = ["query_component", "alias"]

    def __init__(self, query_component: QueryComponent, alias_hint: str = "", exact_alias: str = None):
        # deal with property assignments
        self.query_component: QueryComponent = query_component
        if exact_alias is not None:
            self.alias: AliasStringConst = AliasStringConst(exact_alias)
        else:
            self.alias: AliasStringConst = AliasStringConst(UniqueNameGenerator.generate_table_name(alias_hint))

        analysis_info = ComponentAnalysisInfo(provided_chains=self.query_component.provided_chain,
                                              required_chains=tuple(), depth=query_component.depth)
        super().__init__(analysis_info)

    # -------------------should be defined for each components------------------
    @classmethod
    def generate_from_bank(cls, bank: Bank, cost: int, cost_model: Counter, time_controller: TimeoutController) \
            -> List[SQLComponent]:
        result = []
        queries = Bank.get_from_bank(bank, QueryComponent, cost, cost_model)
        for i in queries:
            assert isinstance(i, QueryComponent)
            candidate = QueryComponentSource(i, alias_hint="cte")
            if candidate.cost <= cost and candidate not in bank:
                result.append(candidate)
        return result

    # determine how a class compute hash
    def __hashables__(self):
        return ["query_component"]

    def _calculate_component_counter(self):
        return self.query_component.component_counter

    def _calculate_all_component(self) -> Set[SQLComponent]:
        return {self} | self.query_component.all_components

    def concretize(self, concretize_environment: ConcretizeEnvrionment = None) -> Tuple[
        SQLComponent, ConcretizeEnvrionment]:
        concretized_source, new_env = self.query_component.concretize(concretize_environment)
        updated_env = new_env.update_table_alias(self.alias.val)
        return QueryComponentSource(concretized_source, exact_alias=str(self.alias.val)), updated_env

    def __str__(self):
        if self.alias.val == "":
            return f"{str(self.query_component)}"
        else:
            return f'({str(self.query_component)}) AS "{str(self.alias)}"'


class TableClauseSource(SourceClause, NonTerminals):
    __slots__ = ["table", "alias"]

    def __init__(self, table: TableClause, alias_hint: str = "", exact_alias: str = None):
        # deal with property assignments
        self.table: TableClause = table
        if exact_alias is not None:
            self.alias: AliasStringConst = AliasStringConst(exact_alias)
        else:
            self.alias: AliasStringConst = AliasStringConst(UniqueNameGenerator.generate_table_name(alias_hint))

        analysis_info = ComponentAnalysisInfo(provided_chains=self.table.provided_chain,
                                              required_chains=tuple())
        super().__init__(analysis_info)

    # -------------------should be defined for each components------------------
    @classmethod
    def generate_from_bank(cls, bank: Bank, cost: int, cost_model: Counter, time_controller: TimeoutController) \
            -> List[SQLComponent]:
        result = []
        tables = Bank.get_from_bank(bank, TableClause, cost, cost_model)
        for i in tables:
            assert isinstance(i, TableClause)
            candidate = TableClauseSource(i, alias_hint=i.table.table_name)
            if candidate not in bank:
                result.append(TableClauseSource(i, alias_hint=i.table.table_name))
        return result

    # determine how a class compute hash
    def __hashables__(self):
        return ["table"]

    def _calculate_component_counter(self):
        return self.table.component_counter

    def _calculate_all_component(self) -> Set[SQLComponent]:
        return {self} | self.table.all_components

    def concretize(self, concretize_environment: ConcretizeEnvrionment = None) -> Tuple[
        SQLComponent, ConcretizeEnvrionment]:
        concretized_source, new_env = self.table.concretize(concretize_environment)
        updated_env = new_env.update_table_alias(self.alias.val)
        return TableClauseSource(concretized_source, exact_alias=str(self.alias)), updated_env

    def __str__(self):
        if self.alias.val == "":
            f"{str(self.table)}"
        else:
            return f'{str(self.table)} AS "{str(self.alias)}"'


class QueryComponent(NonTerminals):
    __slots__ = ["select_clause", "source_clause", "where_clause", "having_clause", "group_by_clause",
                 "order_by_clause", "distinct_clause"]

    def __init__(self, select_clause: SelectClause, source_clause: SourceClause, where_clause: PredicateClause,
                 having_clause: PredicateClause, group_by_clause: GroupByClause, order_by_clause: OrderByClause,
                 distinct_clause: DistinctClause | NoneClause):
        # deal with property assignments
        self.select_clause: SelectClause = select_clause
        self.source_clause: SourceClause = source_clause
        self.where_clause: PredicateClause = where_clause
        self.having_clause: PredicateClause = having_clause
        self.group_by_clause: GroupByClause = group_by_clause
        self.order_by_clause: OrderByClause = order_by_clause
        self.distinct_clause: DistinctClause | NoneClause = distinct_clause

        analysis_info = ComponentAnalysisInfo(provided_chains=self.select_clause.provided_chain,
                                              required_chains=tuple(), depth=source_clause.depth + 1)
        super().__init__(analysis_info)

    # -------------------should be defined for each components------------------
    @classmethod
    def generate_from_bank(cls, bank: Bank, cost: int, cost_model: Counter, time_controller: TimeoutController) \
            -> List[SQLComponent]:
        result = []
        source_clauses = bank.get_from_bank(SourceClause, cost, cost_model,
                                            ConstraintEnvrionment([MaxDepthConstraint(lib.consts.depth_limit - 1)]))
        for source_clause_candidate in source_clauses:
            assert isinstance(source_clause_candidate, SourceClause)
            cost_after_source = cost - source_clause_candidate.cost
            cost_model_after_source = cost_model - source_clause_candidate.component_counter
            constraints_for_where = [FulfilledByProvidedChainsConstraint(source_clause_candidate.provided_chain),
                                     NoAggClauseInPredicateConstraint()]
            where_clauses = bank.get_from_bank(PredicateClause, cost_after_source, cost_model_after_source,
                                               ConstraintEnvrionment(constraints_for_where))
            for where_clause_candidate in where_clauses:
                assert isinstance(where_clause_candidate, PredicateClause)
                cost_after_where = cost - source_clause_candidate.cost - where_clause_candidate.cost
                cost_model_after_where = cost_model - source_clause_candidate.component_counter - \
                                         where_clause_candidate.component_counter
                constraints_for_group_by = [FulfilledByProvidedChainsConstraint(source_clause_candidate.provided_chain)]
                group_by_clauses = bank.get_from_bank(GroupByClause, cost_after_where, cost_model_after_where,
                                                      ConstraintEnvrionment(constraints_for_group_by))
                for group_by_candidate in group_by_clauses:
                    assert isinstance(group_by_candidate, GroupByClause)
                    cost_after_group_by = cost - source_clause_candidate.cost - \
                                          where_clause_candidate.cost - group_by_candidate.cost
                    cost_model_after_group_by = cost_model - source_clause_candidate.component_counter - \
                                                where_clause_candidate.component_counter - \
                                                group_by_candidate.component_counter
                    if isinstance(group_by_candidate, NoneClause):
                        constraints_for_having = [
                            FulfilledByProvidedChainsConstraint(source_clause_candidate.provided_chain),
                            ColumnEitherInGroupByOrAggregationConstraint(tuple())]
                    else:
                        assert isinstance(group_by_candidate, GroupByClauseList)
                        constraints_for_having = [
                            FulfilledByProvidedChainsConstraint(source_clause_candidate.provided_chain),
                            ColumnEitherInGroupByOrAggregationConstraint(group_by_candidate.group_list)]
                    having_clauses = bank.get_from_bank(PredicateClause, cost_after_group_by, cost_model_after_group_by,
                                                        ConstraintEnvrionment(constraints_for_having))
                    for having_candidate in having_clauses:
                        assert isinstance(having_candidate, PredicateClause)
                        cost_after_having = cost - source_clause_candidate.cost - \
                                            where_clause_candidate.cost - group_by_candidate.cost - having_candidate.cost
                        cost_model_after_having = cost_model - source_clause_candidate.component_counter - \
                                                  where_clause_candidate.component_counter - \
                                                  having_candidate.component_counter
                        if not isinstance(group_by_candidate, NoneClause):
                            # have group by or having, then as long as each item fultills either in group by or agg, then we are good
                            constraints_for_select_list_item = [
                                FulfilledByProvidedChainsConstraint(source_clause_candidate.provided_chain),
                                NotNoneConstraint(),
                                ColumnEitherInGroupByOrAggregationConstraint(group_by_candidate.group_list)]
                            select_list_items = bank.get_from_bank(SelectClauseItem, cost_after_having,
                                                                   cost_model_after_having,
                                                                   ConstraintEnvrionment(
                                                                       constraints_for_select_list_item))
                            if len(select_list_items) > 0:
                                select_clause_candidate = SelectClause(tuple(select_list_items))
                                cost_after_select_clause = cost - source_clause_candidate.cost - \
                                                           where_clause_candidate.cost - group_by_candidate.cost - having_candidate.cost \
                                                           - select_clause_candidate.cost
                                cost_model_after_select_clause = cost_model - source_clause_candidate.component_counter - \
                                                                 where_clause_candidate.component_counter - \
                                                                 having_candidate.component_counter - select_clause_candidate.component_counter
                                constraints_for_order_by = [
                                    FulfilledByProvidedChainsConstraint(select_clause_candidate.provided_chain)]

                                order_by_clauses = bank.get_from_bank(OrderByClause, cost_after_select_clause,
                                                                      cost_model_after_select_clause,
                                                                      ConstraintEnvrionment(constraints_for_order_by))
                                for order_by_clause_candidate in order_by_clauses:
                                    assert isinstance(order_by_clause_candidate, OrderByClause)
                                    candidate = QueryComponent(select_clause_candidate,
                                                               source_clause_candidate,
                                                               where_clause_candidate,
                                                               having_candidate,
                                                               group_by_candidate,
                                                               order_by_clause_candidate,
                                                               DistinctClause())
                                    if candidate.cost <= cost and candidate not in bank:
                                        result.append(candidate)
                                    candidate = QueryComponent(select_clause_candidate,
                                                               source_clause_candidate,
                                                               where_clause_candidate,
                                                               having_candidate,
                                                               group_by_candidate,
                                                               order_by_clause_candidate,
                                                               NoneClause())
                                    if candidate.cost <= cost and candidate not in bank:
                                        result.append(candidate)
                        else:
                            # doesn't have group-by, then there are two choices - AggregationOnly or NoAggregationOnly
                            # aggregation only
                            constraints_for_agg_only = [
                                FulfilledByProvidedChainsConstraint(source_clause_candidate.provided_chain),
                                NotNoneConstraint(),
                                AggregationOnlyConstraint()
                            ]
                            select_list_items_agg_only = bank.get_from_bank(SelectClauseItem, cost_after_having,
                                                                            cost_model_after_having,
                                                                            ConstraintEnvrionment(
                                                                                constraints_for_agg_only))
                            if len(select_list_items_agg_only) > 0:
                                select_clause_candidate = SelectClause(tuple(select_list_items_agg_only))
                                cost_after_select_clause = cost - source_clause_candidate.cost - \
                                                           where_clause_candidate.cost - group_by_candidate.cost - having_candidate.cost \
                                                           - select_clause_candidate.cost
                                cost_model_after_select_clause = cost_model - source_clause_candidate.component_counter - \
                                                                 where_clause_candidate.component_counter - \
                                                                 having_candidate.component_counter - select_clause_candidate.component_counter
                                constraints_for_order_by = [
                                    FulfilledByProvidedChainsConstraint(select_clause_candidate.provided_chain)]

                                order_by_clauses = bank.get_from_bank(OrderByClause, cost_after_select_clause,
                                                                      cost_model_after_select_clause,
                                                                      ConstraintEnvrionment(constraints_for_order_by))
                                for order_by_clause_candidate in order_by_clauses:
                                    assert isinstance(order_by_clause_candidate, OrderByClause)
                                    candidate = QueryComponent(select_clause_candidate,
                                                               source_clause_candidate,
                                                               where_clause_candidate,
                                                               having_candidate,
                                                               group_by_candidate,
                                                               order_by_clause_candidate,
                                                               DistinctClause())
                                    if candidate.cost <= cost and candidate not in bank:
                                        result.append(candidate)
                                    candidate = QueryComponent(select_clause_candidate,
                                                               source_clause_candidate,
                                                               where_clause_candidate,
                                                               having_candidate,
                                                               group_by_candidate,
                                                               order_by_clause_candidate,
                                                               NoneClause())
                                    if candidate.cost <= cost and candidate not in bank:
                                        result.append(candidate)

                            # non-aggregation only
                            constraints_for_non_agg_only = [
                                FulfilledByProvidedChainsConstraint(source_clause_candidate.provided_chain),
                                NotNoneConstraint(),
                                NonAggregationOnlyConstraint()
                            ]
                            select_list_items_non_agg_only = bank.get_from_bank(SelectClauseItem,
                                                                                cost_after_having,
                                                                                cost_model_after_having,
                                                                                ConstraintEnvrionment(
                                                                                    constraints_for_non_agg_only))
                            if len(select_list_items_non_agg_only) > 0:
                                select_clause_candidate = SelectClause(tuple(select_list_items_non_agg_only))
                                cost_after_select_clause = cost - source_clause_candidate.cost - \
                                                           where_clause_candidate.cost - group_by_candidate.cost - having_candidate.cost \
                                                           - select_clause_candidate.cost
                                cost_model_after_select_clause = cost_model - source_clause_candidate.component_counter - \
                                                                 where_clause_candidate.component_counter - \
                                                                 having_candidate.component_counter - select_clause_candidate.component_counter
                                constraints_for_order_by = [
                                    FulfilledByProvidedChainsConstraint(select_clause_candidate.provided_chain)]

                                order_by_clauses = bank.get_from_bank(OrderByClause, cost_after_select_clause,
                                                                      cost_model_after_select_clause,
                                                                      ConstraintEnvrionment(constraints_for_order_by))
                                for order_by_clause_candidate in order_by_clauses:
                                    assert isinstance(order_by_clause_candidate, OrderByClause)
                                    candidate = QueryComponent(select_clause_candidate,
                                                               source_clause_candidate,
                                                               where_clause_candidate,
                                                               NoneClause(),
                                                               group_by_candidate,
                                                               order_by_clause_candidate,
                                                               DistinctClause())
                                    if candidate.cost <= cost and candidate not in bank and not is_trivial_select(
                                            candidate):
                                        result.append(candidate)
                                    candidate = QueryComponent(select_clause_candidate,
                                                               source_clause_candidate,
                                                               where_clause_candidate,
                                                               NoneClause(),
                                                               group_by_candidate,
                                                               order_by_clause_candidate,
                                                               NoneClause())
                                    if candidate.cost <= cost and candidate not in bank and not is_trivial_select(
                                            candidate):
                                        result.append(candidate)

        return result

        # determine how a class compute hash

    def __hashables__(self):
        return ["select_clause", "source_clause", "where_clause", "having_clause", "group_by_clause",
                "order_by_clause", "distinct_clause"]

    def _calculate_component_counter(self):
        return self.select_clause.component_counter \
               + self.source_clause.component_counter \
               + self.where_clause.component_counter \
               + self.having_clause.component_counter \
               + self.group_by_clause.component_counter \
               + self.order_by_clause.component_counter \
               + self.distinct_clause.component_counter

    def _calculate_all_component(self) -> Set[SQLComponent]:
        return {self} | self.select_clause.all_components | self.source_clause.all_components | \
               self.where_clause.all_components | self.having_clause.all_components | \
               self.group_by_clause.all_components | self.order_by_clause.all_components | self.distinct_clause.all_components

    def concretize(self, concretize_environment: ConcretizeEnvrionment = None) -> Tuple[
        SQLComponent, ConcretizeEnvrionment]:
        concretized_source, new_env = self.source_clause.concretize(concretize_environment)
        concretized_where_clause, _ = self.where_clause.concretize(new_env)
        concretized_group_clause, _ = self.group_by_clause.concretize(new_env)
        concretized_having_clause, _ = self.having_clause.concretize(new_env)
        concretized_select_clause, exposed_env = self.select_clause.concretize(new_env)
        try:
            concretized_order_clause, _ = self.order_by_clause.concretize(exposed_env)
        except:
            concretized_order_clause = NoneClause()
        concretized_distinct_clause, _ = self.distinct_clause.concretize(exposed_env)
        return QueryComponent(concretized_select_clause,
                              concretized_source,
                              concretized_where_clause,
                              concretized_having_clause,
                              concretized_group_clause,
                              concretized_order_clause,
                              concretized_distinct_clause), exposed_env

    def __str__(self):
        result = ""
        if not isinstance(self.distinct_clause, NoneClause):
            result += f"SELECT DISTINCT {str(self.select_clause)}"
        else:
            result += f"SELECT {str(self.select_clause)}"
        result += f" FROM {str(self.source_clause)}"
        if not isinstance(self.where_clause, NoneClause):
            result += f" WHERE {str(self.where_clause)}"
        if not isinstance(self.group_by_clause, NoneClause):
            result += f" GROUP BY {str(self.group_by_clause)}"
        if not isinstance(self.having_clause, NoneClause):
            result += f" HAVING {str(self.having_clause)}"
        if not isinstance(self.order_by_clause, NoneClause):
            result += f" ORDER BY {str(self.order_by_clause)}"
        return result


class ColChain(ReferencableChain, Terminals):
    __slots__ = ["table", "col"]

    def __init__(self, table: Table, col: str, data_type: DataType):
        self.table: Table = table
        self.col: str = col

        analysis_info = ComponentAnalysisInfo(provided_chains=(self,),
                                              required_chains=(self,),
                                              data_type=data_type)
        super().__init__(analysis_info)

    # -------------------should be defined for each components------------------

    # determine how a class compute hash
    def __hashables__(self):
        return ["table", "col"]

    def _calculate_component_counter(self):
        return Counter({self: 1})

    def _calculate_all_component(self) -> Set[SQLComponent]:
        return {self}

    def concretize(self, concretize_environment: ConcretizeEnvrionment = None) -> Tuple[
        SQLComponent, ConcretizeEnvrionment]:
        resolved_name = concretize_environment.get_link(self)
        if len(resolved_name) == 0:
            raise Exception("can't find matched chain?")
        return RealCol(resolved_name[0].table, resolved_name[0].column, self), concretize_environment

    def __str__(self):
        return f"{str(self.table)}.{str(self.col)}"


class AggChain(ReferencableChain, NonTerminals):
    __slots__ = ["func_name", "child"]

    def __init__(self, func_name: AggregationFuncName, child: ReferencableChain):
        self.func_name: AggregationFuncName = func_name
        self.child: ReferencableChain = child

        analysis_info = ComponentAnalysisInfo(provided_chains=(self,),
                                              required_chains=(self,),
                                              data_type=DataType.Number)
        super().__init__(analysis_info)

    # -------------------should be defined for each components------------------
    @classmethod
    def generate_from_bank(cls, bank: Bank, cost: int, cost_model: Counter, time_controller: TimeoutController) \
            -> List[SQLComponent]:
        result = []
        agg_func_name_candidates = bank.get_from_bank(AggregationFuncName, cost, cost_model)
        for agg_func_name_candidate in agg_func_name_candidates:
            assert isinstance(agg_func_name_candidate, AggregationFuncName)
            new_cost_limit_after_agg = cost - agg_func_name_candidate.cost
            new_cost_model_after_agg = cost_model - agg_func_name_candidate.component_counter
            if isinstance(agg_func_name_candidate, AggregationFuncNameCount):
                constraints = []
            else:
                constraints = [DataTypeInConstraint([DataType.Number])]

            parameter_candidates = bank.get_from_bank(ReferencableChain,
                                                      new_cost_limit_after_agg,
                                                      new_cost_model_after_agg,
                                                      ConstraintEnvrionment(constraints)
                                                      )
            for parameter_candidate in parameter_candidates:
                assert isinstance(parameter_candidate, ReferencableChain)
                candidate = AggChain(agg_func_name_candidate, parameter_candidate)
                if candidate.cost <= cost and candidate not in bank:
                    result.append(candidate)
        return result

    # determine how a class compute hash
    def __hashables__(self):
        return ["func_name", "child"]

    def _calculate_component_counter(self):
        return self.func_name.component_counter + self.child.component_counter + Counter({self: 1})

    def _calculate_all_component(self) -> Set[SQLComponent]:
        return {self} | self.func_name.all_components | self.child.all_components

    def concretize(self, concretize_environment: ConcretizeEnvrionment = None) -> Tuple[
        SQLComponent, ConcretizeEnvrionment]:
        resolved_name = concretize_environment.get_link(self)
        if len(resolved_name) == 0:
            raise Exception("can't find matched chain?")
        return RealCol(resolved_name[0].table, resolved_name[0].column, self), concretize_environment

    def __str__(self):
        return f"{str(self.child)}->{str(self.func_name)}"


class WindowFuncChain(ReferencableChain, NonTerminals):
    __slots__ = ["func_name", "child"]

    def __init__(self, func_name: WindowFuncName, child: Chain):
        self.func_name: WindowFuncName = func_name
        self.child: Chain = child

        analysis_info = ComponentAnalysisInfo(provided_chains=(self,),
                                              required_chains=(self,),
                                              data_type=DataType.Number)
        super().__init__(analysis_info)

    # -------------------should be defined for each components------------------
    @classmethod
    def generate_from_bank(cls, bank: Bank, cost: int, cost_model: Counter, time_controller: TimeoutController) \
            -> List[SQLComponent]:
        result = []
        win_func_name_candidates = bank.get_from_bank(WindowFuncName, cost, cost_model)
        for win_func_name_candidate in win_func_name_candidates:
            assert isinstance(win_func_name_candidate, WindowFuncName)
            new_cost_limit_after_agg = cost - win_func_name_candidate.cost
            new_cost_model_after_agg = cost_model - win_func_name_candidate.component_counter

            parameter_candidates = bank.get_from_bank(Chain,
                                                      new_cost_limit_after_agg,
                                                      new_cost_model_after_agg,
                                                      )
            for parameter_candidate in parameter_candidates:
                assert isinstance(parameter_candidate, Chain)
                candidate = WindowFuncChain(win_func_name_candidate, parameter_candidate)
                if candidate.cost <= cost and candidate not in bank:
                    result.append(candidate)
        return result

    # determine how a class compute hash
    def __hashables__(self):
        return ["func_name", "child"]

    def _calculate_component_counter(self):
        return self.func_name.component_counter + self.child.component_counter + Counter({self: 1})

    def _calculate_all_component(self) -> Set[SQLComponent]:
        return {self} | self.func_name.all_components | self.child.all_components

    def concretize(self, concretize_environment: ConcretizeEnvrionment = None) -> Tuple[
        SQLComponent, ConcretizeEnvrionment]:
        resolved_name = concretize_environment.get_link(self)
        if len(resolved_name) == 0:
            raise Exception("can't find matched chain?")
        return RealCol(resolved_name[0].table, resolved_name[0].column, self), concretize_environment

    def __str__(self):
        return f"{str(self.child)}->{str(self.func_name)}"


class RealCol(Terminals):
    __slots__ = ["table_name", "col_name", "chain"]

    def __init__(self, table_name: str, col_name: str, chain: Chain):
        self.table_name: str = table_name
        self.col_name: str = col_name
        self.chain = chain

        analysis_info = ComponentAnalysisInfo(provided_chains=(chain,),
                                              required_chains=(chain,),
                                              data_type=chain.data_type)
        super().__init__(analysis_info)

    # -------------------should be defined for each components------------------

    # determine how a class compute hash
    def __hashables__(self):
        return ["chain"]

    def _calculate_component_counter(self):
        return Counter({self.chain: 1})

    def concretize(self, concretize_environment: ConcretizeEnvrionment = None) -> Tuple[
        SQLComponent, ConcretizeEnvrionment]:
        raise NotImplementedError("concretize not implemented!")

    def __str__(self):
        if self.table_name != "":
            return f'"{str(self.table_name)}"."{str(self.col_name)}"'
        return f'"{str(self.col_name)}"'

class QueryText(SQLComponent):
    __slots__ = ["query_text"]

    def __init__(self, query_text):
        analysis_info = ComponentAnalysisInfo(provided_chains=tuple(),
                                              required_chains=tuple())
        super().__init__(analysis_info)
        self.query_text = query_text

    # -------------------should be defined for each components------------------

    # determine how a class compute hash
    def __hashables__(self):
        return ["query_text"]

    def _calculate_component_counter(self):
        return Counter({self: 1})

    def _calculate_all_component(self) -> Set[SQLComponent]:
        return {self}

    def concretize(self, concretize_environment: ConcretizeEnvrionment = None) -> Tuple[
        SQLComponent, ConcretizeEnvrionment]:
        raise NotImplementedError("concretize not implemented!")

    def __str__(self):
        return self.query_text

"""
some additional classes
"""


class UniqueIdGenerator:
    cur_id = 0

    @classmethod
    def get_unique_id(cls):
        tmp = cls.cur_id
        cls.cur_id += 1
        return str(tmp)

    @classmethod
    def reset(cls):
        cls.cur_id = 0

class UniqueIdGeneratorForCalCite:
    cur_id = 0

    @classmethod
    def get_unique_id(cls):
        tmp = cls.cur_id
        cls.cur_id += 1
        return str(tmp)

    @classmethod
    def reset(cls):
        cls.cur_id = 0


class Table():
    __slots__ = ["table_name", "table_cols", "unique_identifier"]

    def __init__(self, table_name, table_cols):
        self.table_name = table_name.lower()
        self.table_cols = [(i.lower(), j) for (i, j) in table_cols]
        self.unique_identifier = UniqueIdGenerator.get_unique_id()

    def get_cols(self):
        return self.table_cols

    def __hash__(self):
        return hash(self.table_name + "#" + self.unique_identifier)

    def __str__(self):
        return self.table_name + "[" + self.unique_identifier + "]"

    def __eq__(self, other):
        if isinstance(other, Table):
            if self.unique_identifier == other.unique_identifier:
                return True
        return False


"""
some helper functions
"""


def get_initial_bank(component: SQLComponent) -> List[SQLComponent]:
    result = []
    for k, v in component.component_counter.items():
        if isinstance(k, Terminals):
            result.append(k)
        if isinstance(k, TableClause):
            result += list(k.provided_chain)
    result += list(component.all_components)
    return result


def is_trivial_select(component: QueryComponent):
    if isinstance(component.where_clause, NoneClause) and \
            isinstance(component.having_clause, NoneClause) and \
            isinstance(component.group_by_clause, NoneClause) and \
            isinstance(component.order_by_clause, NoneClause) and \
            set(component.provided_chain).issubset(set(component.source_clause.provided_chain)) and \
            isinstance(component.distinct_clause, NoneClause):
        return True
    return False


def to_chain(component: SQLComponent):
    if isinstance(component, Chain):
        return component
    if isinstance(component, AggregationClause):
        return AggChain(component.func_name, component.parameter)
    if isinstance(component, PredicateOpClause):
        return PredicateOpClause(to_chain(component.larg), component.op, to_chain(component.rarg))
    if isinstance(component, PredicateBinOpLogicClause):
        return PredicateBinOpLogicClause(to_chain(component.larg), component.op, to_chain(component.rarg))
    raise "can't perform to_chain"
