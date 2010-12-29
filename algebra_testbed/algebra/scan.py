import operator
import db.map

Operator = operator.Operator
RANDOM_ACCESS = operator.RANDOM_ACCESS
SEQUENTIAL_ACCESS = operator.SEQUENTIAL_ACCESS
Map = db.map.Map

class Scan(Operator):

    def __init__(self, iterator):
        Operator.__init__(self)
        self._iterator = iterator
        self._stats[RANDOM_ACCESS] = 1

    def open(self):
        pass

    def next(self):
        output_row = None
        try:
            output_row = self._iterator.next()
            self._stats[SEQUENTIAL_ACCESS] += 1
        except StopIteration:
            pass
        return output_row

    def close(self):
        pass
    
    def stats(self):
        return self._stats

class TableScan(Scan):

    def __init__(self, map):
        iterator = iter(map)
        Scan.__init__(self, iterator)

class IndexScan(Scan):

    def __init__(self, index, key = None):
        if key:
            iterator = iter(index.lookup(key))
        else:
            iterator = iter(index)
        Scan.__init__(self, iterator)
