import operator

Operator = operator.Operator

class Scan(Operator):

    def __init__(self, cursor):
        Operator.__init__(self)
        self._cursor = cursor
        self._done = False
        self.count_random_access()

    def open(self):
        pass

    def next(self):
        output_row = None
        if not self._done:
            output_row = self._cursor.next()
            self.count_sequential_access()
            if output_row is None:
                self._done = True
        return output_row

    def close(self):
        self._done = True
    
    def stats(self):
        return self._stats
