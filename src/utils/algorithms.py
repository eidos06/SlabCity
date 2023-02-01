import itertools

def get_all_combinations_below_n(l, n):
    result = []
    for i in range(1, n+1):
        result += list(itertools.combinations(l, i))
    return result
