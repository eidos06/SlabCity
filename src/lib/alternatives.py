from lib.bank import Bank
from typing import List, Tuple
from lib.dsl import SQLComponent, PartitionClauseList, GroupByClauseList, Chain, ColChain, TableClause, AggChain, \
    PredicateOpClause, OpEq, OpLt, OpLeq, ReferencableChain, OrderByClauseList, OrderByClauseItem, OrderByConfigurationASC
from collections import Counter
from lib.cost_estimater import CostEstimater
import itertools

class AlternativeRule:
    @classmethod
    def generate_alternatives(cls, bank:Bank)->Tuple[List[SQLComponent], Counter]:
        raise NotImplementedError("This alternatives is not implemented")

class PartitionByToGroupByAlternativeRule(AlternativeRule):
    @classmethod
    def generate_alternatives(cls, bank:Bank) ->Tuple[List[SQLComponent], List[Counter]]:
        alternative_clauses:List[SQLComponent] = []
        alternative_counters:List[Counter] = []
        partition_by_clauses = bank.get_from_bank(PartitionClauseList, 0, CostEstimater.template_counter)
        for i in partition_by_clauses:
            assert isinstance(i, PartitionClauseList)
            alternative_group_by_clause = GroupByClauseList(i.items)
            alternative_group_by_counter = alternative_group_by_clause.component_counter
            alternative_clauses.append(alternative_group_by_clause)
            alternative_counters.append(alternative_group_by_counter)
        return alternative_clauses, alternative_counters

class GroupByToPartitionByRule(AlternativeRule):
    @classmethod
    def generate_alternatives(cls, bank:Bank) ->Tuple[List[SQLComponent], List[Counter]]:
        alternative_clauses:List[SQLComponent] = []
        alternative_counters:List[Counter] = []
        group_by_clauses = bank.get_from_bank(GroupByClauseList, 0, CostEstimater.template_counter)
        for i in group_by_clauses:
            assert isinstance(i, GroupByClauseList)
            alternative_group_by_clause = PartitionClauseList(i.group_list)
            alternative_group_by_counter = alternative_group_by_clause.component_counter
            alternative_clauses.append(alternative_group_by_clause)
            alternative_counters.append(alternative_group_by_counter)
        return alternative_clauses, alternative_counters

class GroupBySameColNameAlternativeRule(AlternativeRule):
    @classmethod
    def get_alternative_chains(cls, chain: Chain, bank: Bank) -> List[Chain]:
        if isinstance(chain, ColChain):
            result = []
            tables = bank.get_from_bank(TableClause, 0, CostEstimater.template_counter)
            for t in tables:
                for chain_candidate in t.provided_chain:
                    if chain.col == chain_candidate.col:
                        result.append(chain_candidate)
            return result
        if isinstance(chain, AggChain):
            result = []
            result_for_inner = GroupBySameColNameAlternativeRule.get_alternative_chains(chain.child, bank)
            for r in result_for_inner:
                result.append(AggChain(chain.func_name, r))
            return result
        return [chain]

    @classmethod
    def generate_alternatives(cls, bank:Bank) ->Tuple[List[SQLComponent], List[Counter]]:
        alternative_clauses:List[SQLComponent] = []
        alternative_counters:List[Counter] = []
        group_by_clauses = bank.get_from_bank(GroupByClauseList, 0, CostEstimater.template_counter)
        for i in group_by_clauses:
            assert isinstance(i, GroupByClauseList)
            alternative_lists = []
            for j in i.group_list:
                alternative_lists.append(GroupBySameColNameAlternativeRule.get_alternative_chains(j, bank))
            for new_list in itertools.product(*alternative_lists):
                alternative_clause = GroupByClauseList(new_list)
                alternative_group_by_counter = alternative_clause.component_counter
                alternative_clauses.append(alternative_clause)
                alternative_counters.append(alternative_group_by_counter)
        return alternative_clauses, alternative_counters

class GroupBySubsetAlternativesRule(AlternativeRule):
    @classmethod
    def get_all_subset(cls, input_list) -> List[Tuple[SQLComponent]]:
        result = []
        for i in range(1, len(input_list)):
            result += list(itertools.combinations(input_list, i))
        return result

    @classmethod
    def generate_alternatives(cls, bank: Bank) -> Tuple[List[SQLComponent], List[Counter]]:
        alternative_clauses: List[SQLComponent] = []
        alternative_counters: List[Counter] = []
        group_by_clauses = bank.get_from_bank(GroupByClauseList, 0, CostEstimater.template_counter)
        for i in group_by_clauses:
            assert isinstance(i, GroupByClauseList)
            for candidate in GroupBySubsetAlternativesRule.get_all_subset(i.group_list):
                alternative_group_by_clause = GroupByClauseList(candidate)
                alternative_group_by_counter = alternative_group_by_clause.component_counter
                alternative_clauses.append(alternative_group_by_clause)
                alternative_counters.append(alternative_group_by_counter)
        return alternative_clauses, alternative_counters

class GroupByTransitiveRule(AlternativeRule):
    @classmethod
    def generate_alternatives(cls, bank: Bank) -> Tuple[List[SQLComponent], List[Counter]]:
        alternative_clauses: List[SQLComponent] = []
        alternative_counters: List[Counter] = []
        group_by_clauses = bank.get_from_bank(GroupByClauseList, 0, CostEstimater.template_counter)
        equalities = bank.get_from_bank(PredicateOpClause, 0, CostEstimater.template_counter)
        # the purpose is to generate new group by clause if it is in one side of the equalities
        #
        for i in group_by_clauses:
            assert isinstance(i, GroupByClauseList)
            if len(i.group_list) == 1:
                reference = i.group_list[0]
                for eqs in equalities:
                    if isinstance(eqs,PredicateOpClause) and isinstance(eqs.op, OpEq):
                        if eqs.larg == reference:
                            alternative_group_by_clause = GroupByClauseList(tuple([eqs.rarg]))
                            alternative_group_by_counter = alternative_group_by_clause.component_counter
                            alternative_clauses.append(alternative_group_by_clause)
                            alternative_counters.append(alternative_group_by_counter)
                            continue
                        if eqs.rarg == reference:
                            alternative_group_by_clause = GroupByClauseList(tuple([eqs.larg]))
                            alternative_group_by_counter = alternative_group_by_clause.component_counter
                            alternative_clauses.append(alternative_group_by_clause)
                            alternative_counters.append(alternative_group_by_counter)
                            continue
        return alternative_clauses, alternative_counters

class LtToOrderByRule(AlternativeRule):
    @classmethod
    def generate_alternatives(cls, bank: Bank) -> Tuple[List[SQLComponent], List[Counter]]:
        alternative_clauses: List[SQLComponent] = []
        alternative_counters: List[Counter] = []
        predicate_clause = bank.get_from_bank(PredicateOpClause, 0, CostEstimater.template_counter)
        # the purpose is to generate new group by clause if it is in one side of the equalities
        #
        for i in predicate_clause:
            if isinstance(i,PredicateOpClause) and (isinstance(i.op, OpLt) or isinstance(i.op, OpLeq)):
                if isinstance(i.larg, ReferencableChain):
                    alternative_group_by_clause = OrderByClauseList(tuple([OrderByClauseItem(i.larg, OrderByConfigurationASC())]))
                    alternative_group_by_counter = alternative_group_by_clause.component_counter
                    alternative_clauses.append(alternative_group_by_clause)
                    alternative_counters.append(alternative_group_by_counter)
                if isinstance(i.rarg, ReferencableChain):
                    alternative_group_by_clause = OrderByClauseList(
                        tuple([OrderByClauseItem(i.rarg, OrderByConfigurationASC())]))
                    alternative_group_by_counter = alternative_group_by_clause.component_counter
                    alternative_clauses.append(alternative_group_by_clause)
                    alternative_counters.append(alternative_group_by_counter)
                    continue
        return alternative_clauses, alternative_counters

