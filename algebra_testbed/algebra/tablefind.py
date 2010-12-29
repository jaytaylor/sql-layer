import operator
import db.map

Operator = operator.Operator
RANDOM_ACCESS = operator.RANDOM_ACCESS
SEQUENTIAL_ACCESS = operator.SEQUENTIAL_ACCESS
Map = db.map.Map

# Reference does not include table_key parameter. Included because testbed does not
# model hkeys accurately.

class TableFind(Operator):

    def __init__(self, input, table_key, table):
        Operator.__init__(self)
        self._input = input
        self._table_key = table_key
        self._table = table

    def open(self):
        self._input.open()

    def next(self):
        output_row = None
        input_row = self._input.next()
        if input_row:
            key = [input_row[field] for field in self._table_key]
            output_row = self._table.lookup(key)
            assert len(output_row) == 1
            self._stats[RANDOM_ACCESS] += 1
        return output_row

    def close(self):
        self._input.close()

    def stats(self):
        return self._stats.merge(self._input.stats())
