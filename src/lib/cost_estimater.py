from __future__ import annotations
from collections import Counter
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from lib.dsl import SQLComponent
import logging


class CostEstimater:
    # this variable defines some information used to calculate the preference score
    template_counter: Counter = None

    @classmethod
    def set_preference_template(cls, template: SQLComponent):
        logging.debug(f"template counter set:{str(template.component_counter)}")
        cls.template_counter = template.component_counter

    @classmethod
    def update_allow_cost_model(cls, counter:Counter):
        cls.template_counter += counter

    @classmethod
    def get_cost(cls, query: SQLComponent, cost_model: Counter=None, enforce_recalculate=False):
        if cost_model is None:
            cost_model = cls.template_counter
        if cls.template_counter is None:
            raise Exception("template query not set!")
        if cost_model == cls.template_counter and not enforce_recalculate:
            return query.cost

        # G:generated, Q:original query
        # PosSum(components(G)- Component(Q) U alternatives(components(Q)))
        # note: we don't count cost for chains themselves since they can be referenced anywhere

        scores = 0

        minus_result = query.component_counter - cost_model
        from lib.dsl import Chain, AggregationFuncName
        for k, v in minus_result.items():
            if isinstance(k, Chain) or isinstance(k, AggregationFuncName):
                if k in cls.template_counter:
                    continue
                scores += 1
            else:
                scores += v

        return scores
