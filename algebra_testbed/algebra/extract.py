import operator
import db.map
import db.row

Operator = operator.Operator
Map = db.map.Map
Row = db.row.Row

class Extract(Operator):

    def __init__(self, input, rowtype):
        Operator.__init__(self)
        self._input = input
        self._rowtype = rowtype

    def open(self):
        self._input.open()

    def next(self):
        output_row = None
        input_row = None
        first = True
        while (input_row is not None and output_row is None) or first:
            input_row = self._input.next()
            if input_row:
                output_row = self.extract_row(input_row)
            first = False
        return output_row

    def close(self):
        self._input.close()

    def stats(self):
        return self._input.stats()

    def extract_row(self, input_row):
        if self._rowtype.ancestor_of(input_row.rowtype):
            output_row = input_row
        else:
            output_row = None
        return output_row
