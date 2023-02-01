class Benchmark:

    def __init__(self,
                 problem_id, query_id,
                 input_cost, input_time):
        self.__problem_id = int(problem_id)
        self.__query_id = int(query_id)
        self.__input_cost = float(input_cost)
        self.__input_time = float(input_time)

    def get_problem_id(self):
        return self.__problem_id

    def get_query_id(self):
        return self.__query_id

    def get_input_measure(self, measure):
        if measure == 'cost':
            return self.__input_cost
        elif measure == 'time':
            return self.__input_time
        else:
            assert False

    def __get_measure(self, cost, time, measure):
        if measure == 'cost':
            return cost
        elif measure == 'time':
            return time
        else:
            assert False, measure

    def set_tool_result(self, tool, solved, cost=None, time=None):
        if tool == 'SC':
            self.__SC_solved = solved
            if solved:
                self.__SC_cost = float(cost)
                self.__SC_time = float(time)
        elif tool == 'LR':
            self.__LR_solved = solved
            if solved:
                self.__LR_cost = float(cost)
                self.__LR_time = float(time)
        elif tool == 'WT':
            self.__WT_solved = solved
            if solved:
                self.__WT_cost = float(cost)
                self.__WT_time = float(time)
        elif tool == 'GT':
            self.__GT_solved = solved
            if solved:
                self.__GT_cost = float(cost)
                self.__GT_time = float(time)
        else:
            assert False

    def get_tool_measure(self, tool, measure):
        if tool == 'SC':
            if self.__SC_solved:
                return self.__get_measure(self.__SC_cost, self.__SC_time, measure)
            else:
                return ''
        elif tool == 'LR':
            if self.__LR_solved:
                return self.__get_measure(self.__LR_cost, self.__LR_time, measure)
            else:
                return ''
        elif tool == 'WT':
            if self.__WT_solved:
                return self.__get_measure(self.__WT_cost, self.__WT_time, measure)
            else:
                return ''
        elif tool == 'GT':
            if self.__GT_solved:
                return self.__get_measure(self.__GT_cost, self.__GT_time, measure)
            else:
                return ''
        else:
            assert False

    def get_tool_solved(self, tool):
        if tool == 'SC':
            return self.__SC_solved
        elif tool == 'LR':
            return self.__LR_solved
        elif tool == 'WT':
            return self.__WT_solved
        elif tool == 'GT':
            return self.__GT_solved
        else:
            assert False

    def tool_output_smaller_measure(self, tool, measure):
        return self.get_tool_solved(tool) and self.get_tool_measure(tool, measure) < self.get_input_measure(measure)

    def tool_output_bigger_measure(self, tool, measure):
        return self.get_tool_solved(tool) and self.get_tool_measure(tool, measure) >= self.get_input_measure(measure)

    def tool_output_unsolved(self, tool):
        return not self.get_tool_solved(tool)
