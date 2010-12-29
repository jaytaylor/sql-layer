N_STATS = 3
RANDOM_ACCESS = 0
SEQUENTIAL_ACCESS = 1
SORT = 2

class Stats(object):

    def __init__(self):
        self._stats = [0] * N_STATS

    def __getitem__(self, i):
        return self._stats[i]

    def __setitem__(self, i, x):
        self._stats[i] = x

    def merge(self, other):
        merged = Stats()
        for i in xrange(N_STATS):
            merged[i] = self[i] + other[i]
        return merged

class Operator(object):

    def __init__(self):
        self._stats = Stats()

    def open(self):
        assert False

    def next(self):
        assert False

    def close(self):
        assert False

    def stats(self):
        assert False
