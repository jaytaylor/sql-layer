import operator

Operator = operator.Operator

# Reference does not include table_key parameter. Included here
# because testbed does not model hkeys accurately.

_DONE = object()

class IndexedTableScan(Operator):

    def __init__(self, input, table_key, table):
        Operator.__init__(self)
        self._input = input
        self._table_key = table_key
        self._table = table
        self._cursor = None
        self._index_row = None

    def open(self):
        self._input.open()

    def next(self):
        table_row = None
        while table_row is None and self._index_row is not _DONE:
            if self._index_row is None:
                self._index_row = self._input.next()
            if self._index_row is None:
                self._index_row = _DONE
            else:
                if not self._cursor:
                    key = [self._index_row[field]
                           for field in self._table_key]
                    self._cursor = self._table.cursor(key, None)
                    self.count_random_access()
                table_row = self._cursor.next()
                self.count_sequential_access()
                if table_row:
                    if not self._index_row.ancestor_of(table_row):
                        table_row = None
                        self._index_row = None
                        self._cursor = None
                else:
                    self._index_row = None
                    self._cursor = None
        return table_row

    def close(self):
        self._input.close()

    def stats(self):
        return self._stats.merge(self._input.stats())
