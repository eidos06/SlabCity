from lib.dsl import *
from typing import Tuple
from lib.exceptions import TrimNotFulfilledException
import itertools
from lib.bank import Bank


# trim a query component by leaving only necessary chains

def trim(query: QueryComponent, necessary_chains: Tuple[Chain], bank: Bank, original_names: Tuple[str]) -> List[QueryComponent]:
    # return query
    # if len(set(necessary_chains) - set(query.provided_chain)) == 0:
    #     print("!")

    if not set(necessary_chains).issubset(set(query.provided_chain)):
        raise TrimNotFulfilledException()
    queries: List[QueryComponent] = _trim(query, necessary_chains)
    # reset the select order and output col name
    result = []
    for q in queries:
        assert isinstance(q, QueryComponent)
        new_select_list:List[SelectClauseItemWithAlias] = []
        for idx, c in enumerate(necessary_chains):
            for i in q.select_clause.items:
                if c == i.provided_chain[0]:
                    new_select_list.append(SelectClauseItemWithAlias(i, original_names[idx]))
        q.select_clause = SelectClause(tuple(new_select_list))
        result.append(q)
    return result



def _trim(query: QueryComponent, necessary_chains: Tuple[Chain]) -> List[QueryComponent]:
    if isinstance(query, QueryComponent):
        # first, leave only chains in targetlist that has a match
        dict_necessary_chains = {}
        for c in query.select_clause.items:
            if c.provided_chain[0] in necessary_chains:
                dict_necessary_chains[c.provided_chain[0]] = dict_necessary_chains.get(c.provided_chain[0], []) + [c]

        # combine different possibilities
        tmp_combination_list = []
        for v in dict_necessary_chains.values():
            tmp_combination_list.append(v)

        returned = []

        for new_select_items in itertools.product(*tmp_combination_list):
            if len(new_select_items) == 0:
                raise TrimNotFulfilledException()
            # build new necessary chains
            new_necessary_chains = []
            for c in new_select_items:
                new_necessary_chains += c.required_chain
            new_necessary_chains += query.group_by_clause.required_chain
            new_necessary_chains += query.where_clause.required_chain
            new_necessary_chains += query.having_clause.required_chain
            new_necessary_chains += query.order_by_clause.required_chain
            new_necessary_chains = tuple(set(new_necessary_chains))
            source_clauses = _trim(query.source_clause, new_necessary_chains)
            for source_clause in source_clauses:
                returned.append(QueryComponent(select_clause=SelectClause(new_select_items),
                                               source_clause=source_clause,
                                               where_clause=query.where_clause,
                                               group_by_clause=query.group_by_clause,
                                               having_clause=query.having_clause,
                                               order_by_clause=query.order_by_clause,
                                               distinct_clause=query.distinct_clause
                                               ))
        return returned
    if isinstance(query, QueryComponentSource):
        returned = []
        for i in _trim(query.query_component, necessary_chains):
            returned.append(QueryComponentSource(i, exact_alias=query.alias))
        return returned
    if isinstance(query, TableClauseSource):
        return [query]
    if isinstance(query, JoinWithOnClause):
        returned = []
        new_necessary_chains = tuple(necessary_chains + query.on_clause.required_chain)
        left_candidates = _trim(query.larg, new_necessary_chains)
        right_candidates = _trim(query.rarg, new_necessary_chains)
        for l in left_candidates:
            for r in right_candidates:
                returned.append(JoinWithOnClause(l, r,
                                                 query.join_type, query.on_clause))
        return returned
