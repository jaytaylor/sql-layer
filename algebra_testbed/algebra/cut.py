import operator
import db.map
import db.row

Operator = operator.Operator
Map = db.map.Map
Row = db.row.Row

class Cut(Operator):

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
                output_row = self.cut_row(input_row)
            first = False
        return output_row

    def close(self):
        self._input.close()

    def stats(self):
        return self._input.stats()

    def cut_row(self, input_row):
        if self._rowtype.ancestor_of(input_row.rowtype):
            output_row = None
        else:
            output_row = input_row
        return output_row
