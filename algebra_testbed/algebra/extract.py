import operator

UnaryOperator = operator.UnaryOperator

class Extract(UnaryOperator):

    def __init__(self, input, rowtype):
        UnaryOperator.__init__(self, input)
        self._rowtype = rowtype

    def handle_row(self, row):
        if self._rowtype.ancestor_of(row.rowtype):
            output_row = row
        else:
            output_row = None
        return output_row
