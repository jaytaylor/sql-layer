import operator

UnaryOperator = operator.UnaryOperator

class Select(UnaryOperator):

    def __init__(self, input, predicate_rowtype, predicate):
        UnaryOperator.__init__(self, input)
        self._predicate_rowtype = predicate_rowtype
        self._predicate = predicate
        self._selected = False
        self._examined_row = None

    def handle_row(self, row):
        output_row = None
        if row.rowtype is self._predicate_rowtype:
            self._selected = self._predicate(row)
            self._examined_row = row
            if self._selected:
                output_row = row
        elif self._predicate_rowtype.ancestor_of(row.rowtype):
            if self._examined_row and self._examined_row.ancestor_of(row):
                # row is not an orphan
                if self._selected:
                    output_row = row
            else:
                # row is an orphan
                self._selected = False
                self._examined_row = None
        else: # row.rowtype is ancestor or unrelated
            output_row = row
        return output_row
