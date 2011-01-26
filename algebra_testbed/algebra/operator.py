N_STATS = 3
RANDOM_ACCESS = 0
SEQUENTIAL_ACCESS = 1
SORT = 2

class _Node(object):

    def __init__(self, object):
        self.object = object
        self.next = None

class Pending(object):

    def __init__(self):
        self.head = None
        self.tail = None

    def add(self, object):
        node = _Node(object)
        if self.head is None:
            self.head = node
            self.tail = node
        else:
            self.tail.next = node
            self.tail = node

    def take(self):
        object = None
        if self.head:
            object = self.head.object
            self.head = self.head.next
            if self.head is None:
                self.tail = None
        return object

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

    def count_random_access(self):
        self._stats[RANDOM_ACCESS] += 1

    def count_sequential_access(self):
        self._stats[SEQUENTIAL_ACCESS] += 1

    def count_sort(self, n):
        self._stats[SORT] += n

class UnaryOperator(Operator):

    def __init__(self, input):
        Operator.__init__(self)
        self._input = input

    def open(self):
        self._input.open()

    def next(self):
        output_row = None
        input_row = self._input.next()
        while input_row is not None and output_row is None:
            output_row = self.handle_row(input_row)
            if output_row is None:
                input_row = self._input.next()
        return output_row

    def close(self):
        self._input.close()

    def stats(self):
        return self._stats.merge(self._input.stats())

    def handle_row(self, row):
        assert False
