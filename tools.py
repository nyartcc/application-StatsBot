import functools


def convertTuple(tup):
    str = functools.reduce(operator.add, (tup))
    return str
