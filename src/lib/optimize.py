
import logging
import traceback
from pglast.ast import Node
from lib.basics import Schema
from lib.synthesize import synthesis
from pglast.stream import RawStream
import time
from utils.time_collector import TimeCollector

def optimize(query: Node, schema:Schema, oracle):
    list_crash = []
    num_total_generated = 0
    num_filtered_after_oracle = 0
    num_oracle_break = 0
    total_synthesis_time = 0
    total_oracle_test_time = 0
    slow_query_text = RawStream()(query)
    oracle.init_generator(slow_query_text)
    oracle.setup_sqlite()

    counter_examples = []

    last_synthesis_time = time.perf_counter()

    for candidate in synthesis(query, schema, []):
        total_synthesis_time += time.perf_counter() - last_synthesis_time
        num_total_generated += 1
        try:
            last_oracle_test_time = time.perf_counter()
            # equiv_result, databases = oracle.test(str(candidate), slow_query_text, counter_examples)
            equiv_result, databases = oracle.test(str(candidate), slow_query_text, counter_examples)
            total_oracle_test_time += time.perf_counter() - last_oracle_test_time
            if equiv_result:
                num_filtered_after_oracle += 1
                TimeCollector.equivalent_queries_synthesized += 1
                # logging.warning("Oracle Passed")
                # logging.warning("query_passed:")
                # logging.warning(str(candidate))
                yield candidate, num_total_generated, num_filtered_after_oracle, num_oracle_break, total_synthesis_time, total_oracle_test_time
            else:
                counter_examples = databases
                # logging.warning("Oracle Rejected")
                # logging.warning("query_rejected:")
                # logging.warning(str(candidate))
        except Exception as e:
            # logging.warning("Oracle Break: " + str(e))
            # logging.warning("query info are below:")
            # logging.warning(f"slow query: {slow_query_text}")
            # logging.warning(f"generated query: {str(candidate)}")
            # logging.warning(f"traceback:")
            # logging.warning(traceback.format_exc())

            num_oracle_break += 1
            total_oracle_test_time += time.perf_counter() - last_oracle_test_time
            yield candidate, num_total_generated, num_filtered_after_oracle, num_oracle_break, total_synthesis_time, total_oracle_test_time
        last_synthesis_time = time.perf_counter()     
    oracle.clear_counterexample_sqlite(counter_examples)   
    oracle.clean_sqlite()
    # oracle.clear_counterexample(counter_examples)