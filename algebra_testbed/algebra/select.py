import operator
import db.map
import db.row

Operator = operator.Operator
Map = db.map.Map
Row = db.row.Row

class Select(Operator):

    def __init__(self, input, predicate_rowtype, predicate):
        Operator.__init__(self)
        self._input = input
        self._predicate_rowtype = predicate_rowtype
        self._predicate = predicate
        self._selected = False
        self._examined_row = None

    def open(self):
        self._input.open()

    def next(self):
        output_row = None
        input_row = None
        first = True
        while (input_row is not None and output_row is None) or first:
            input_row = self._input.next()
            if input_row:
                output_row = self.select_row(input_row)
            first = False
        return output_row

    def close(self):
        self._input.close()

    def stats(self):
        return self._input.stats()

    def select_row(self, input_row):
        output_row = None
        if input_row.rowtype is self._predicate_rowtype:
            self._selected = self._predicate(input_row)
            self._examined_row = input_row
            if self._selected:
                output_row = input_row
        elif self._predicate_rowtype.ancestor_of(input_row.rowtype):
            if self._examined_row and self._examined_row.ancestor_of(input_row):
                # input_row is not an orphan
                if self._selected:
                    output_row = input_row
            else:
                # input_row is an orphan
                self._selected = False
                self._examined_row = None
        else: # row.rowtype is ancestor or unrelated
            output_row = input_row
        return output_row
