from pglast.ast import Node
from lib.basics import Schema, TimeoutController
from lib.exceptions import TrimNotFulfilledException
from lib.translation import pglast_to_dsl
from lib.cost_estimater import CostEstimater
from lib.bank import Bank
from lib.helpers import is_superset
from lib.dsl import NonTerminals, QueryComponent, get_initial_bank, UniqueIdGenerator, UniqueNameGenerator, \
    UniqueNameGeneratorForConcretize, Chain, ColChain, AggChain, TableClause, QueryText
from lib.alternatives import AlternativeRule
from lib.trim import trim
import lib.class_order
from typing import List, Tuple
from lib.alternatives import PartitionByToGroupByAlternativeRule,\
GroupBySubsetAlternativesRule,GroupByTransitiveRule,GroupByTransitiveRule,\
GroupByTransitiveRule,GroupBySameColNameAlternativeRule,\
GroupByToPartitionByRule,LtToOrderByRule
import random
import logging
import itertools
from pglast.ast import SelectStmt, ResTarget, ColumnRef, A_Const, Null, RangeSubselect, A_Expr, String, Integer, Alias
from pglast.enums import A_Expr_Kind
from utils.pglast_preprocess import preprocessing
from utils.deepcopy import deepcopy
from pglast.stream import RawStream
import time

from utils.time_collector import TimeCollector

def synthesis(query: Node, schema: Schema, alternative_rules: List[AlternativeRule]):
    """

    Args:
        query: the query to be optimized
        schema: the schema of the query
        alternative_rules: a list of alternative rules
        timeout: timeout in seconds

    Returns:
        yield newly generated components - note: in theory we can generate infinite components,
        so here we use yield to avoid explicitly list all components
    """
    # init alternative rule
    alternative_rules:List[AlternativeRule] = [PartitionByToGroupByAlternativeRule(),
                                               GroupBySubsetAlternativesRule(),
                                               GroupByTransitiveRule(),
                                               GroupBySameColNameAlternativeRule(),
                                               GroupByToPartitionByRule(),
                                               LtToOrderByRule()]

    # init timeout controller
    random.seed(0)
    UniqueIdGenerator.reset()

    # first we translate the query from ast to data provanence representations
    # we do this because we need to abstract the query instead of using the original one
    # query = preprocessing(query, schema)
    
    start = time.time()
    tmp = generate_false_candidate_regardless_of_parsing(query)
    end = time.time()
    TimeCollector.time_synthesis += end-start
    TimeCollector.query_searched += 1
    yield tmp

    query_dsl, _ = pglast_to_dsl(query, schema)
    CostEstimater.set_preference_template(query_dsl)

    # generate terminals (constants, tables) from original query
    # we now restrict that all constants should come from the original query
    # also get components from the original query and put here
    # our assumption is that those components have a cost of 0
    # and it is very likely that newly generated query will use them
    terminals = get_initial_bank(query_dsl)
    bank = Bank()
    for i in terminals:
        if i.cost <= 0:
            bank.add_to_bank(i)
    for i in lib.class_order.terminals_to_init:
        if i.cost <= 0:
            bank.add_to_bank(i)
            
    TimeCollector.last_synthesis_time = time.time()
    necessary_chains_list = get_alternative_necessary_chains(query_dsl.provided_chain, bank)
    original_output_col_names = get_original_output_col_names(query)
    #check if initialized bank has some outputs
    for necessary_chains in necessary_chains_list:
        for i in check_if_init_has_output(bank, necessary_chains, bank, original_output_col_names):
            TimeCollector.query_searched += 1
            TimeCollector.time_synthesis += time.time()-TimeCollector.last_synthesis_time
            yield i
            TimeCollector.last_synthesis_time = time.time()
    # init cost estimator based on preference template
    cost_limit = 0

    # apply alternative rules
    for rule in alternative_rules:
        alternative_components, alternative_cost_model = rule.generate_alternatives(bank)
        for ac in alternative_components:
            bank.add_to_bank(ac)
        for acm in alternative_cost_model:
            CostEstimater.update_allow_cost_model(acm)

    TimeCollector.last_synthesis_time = time.time()
    while True:
        new_bank = Bank()
        for nt in lib.class_order.non_terminals:
            for i in nt.generate_from_bank(bank, cost_limit, CostEstimater.template_counter, TimeoutController(1)):
                new_bank.add_to_bank(i)
                if isinstance(i, QueryComponent):
                    UniqueNameGenerator.reset()
                    UniqueNameGeneratorForConcretize.reset()
                    for necessary_chains in necessary_chains_list:
                        try:
                            trimmed_is = trim(i, necessary_chains, bank, original_output_col_names)
                            for trimmed_i in trimmed_is:
                                concretized_i, _ = trimmed_i.concretize()
                                TimeCollector.query_searched += 1
                                TimeCollector.time_synthesis += time.time()-TimeCollector.last_synthesis_time
                                yield concretized_i
                                TimeCollector.last_synthesis_time = time.time()
                        except TrimNotFulfilledException as e:
                            continue
        if new_bank.empty():
            # saturated, continue to next cost
            cost_limit = cost_limit + 1
            continue
        bank.merge(new_bank)


def check_if_init_has_output(init_bank:Bank, necessary_chains, bank, original_output_col_names):
    query_components = init_bank.get_from_bank(QueryComponent, 0, CostEstimater.template_counter)
    results = []
    for i in query_components:
        UniqueNameGenerator.reset()
        UniqueNameGeneratorForConcretize.reset()
        try:
            trimmed_is = trim(i, necessary_chains, bank, original_output_col_names)
            for trimmed_i in trimmed_is:
                concretized_i, _ = trimmed_i.concretize()
                results.append(concretized_i)
        except TrimNotFulfilledException as e:
            pass
    return results

def get_alternative_necessary_chains(necessary_chains:Tuple[Chain,...], bank:Bank) -> List[Tuple[Chain,...]]:
    if len(necessary_chains) > 8:
        return [necessary_chains]
    alternatives = []
    for i in necessary_chains:
        alternatives.append(get_alternative_chains(i, bank))
    all_necessary_chains = list(itertools.product(*alternatives))
    return all_necessary_chains

def get_alternative_chains(chain:Chain, bank:Bank)->List[Chain]:
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
        result_for_inner = get_alternative_chains(chain.child, bank)
        for r in result_for_inner:
           result.append(AggChain(chain.func_name, r))
        return result
    return [chain]

def get_original_output_col_names(query:Node)->Tuple[str]:
    assert isinstance(query, SelectStmt)
    result = []
    for i in query.targetList:
        assert isinstance(i, ResTarget)
        if i.name is not None:
            result.append(i.name)
            continue
        value = i.val
        if isinstance(value, ColumnRef):
            result.append(value.fields[-1].val)
            continue
        result.append("tmp")
    return result

def remove_table_name_for_res_target(node:ResTarget):
    if node.name is not None:
        return ResTarget(val=ColumnRef(fields=(String(node.name),)))
    return ResTarget(val=ColumnRef(fields=(String(node.val.fields[-1].val),)))
    # if isinstance(node.val,ColumnRef):
    #     if len(node.val.fields) > 1:
    #         return ResTarget(name=node.name,val=ColumnRef(fields=(node.val.fields[-1],)))
    # return node

def generate_false_candidate_regardless_of_parsing(node:Node):
    assert isinstance(node,SelectStmt)
    len_cols = len(node.targetList)
    col_names = []
    for i in node.targetList:
        if i.name is not None:
            col_names.append(String(i.name))
        else:
            col_names.append(String(i.val.fields[-1].val))
    value_lists = SelectStmt(valuesLists=tuple([tuple([A_Const(val=Null())]*len_cols)]))
    new_target_list = tuple([remove_table_name_for_res_target(i) for i in node.targetList])

    return_query_node:Node = SelectStmt(targetList=new_target_list,
                                        fromClause=(RangeSubselect(subquery=value_lists, alias=Alias("T_cte_empty",colnames=tuple(col_names))),),
                                        whereClause=A_Expr(kind=A_Expr_Kind.AEXPR_OP, name=(String("="),), lexpr=A_Const(val=Integer(1)), rexpr=A_Const(val=Integer(0))))
    return QueryText(RawStream()(return_query_node))



