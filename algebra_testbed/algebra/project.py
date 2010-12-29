import operator
import db.map
import db.row

Operator = operator.Operator
Map = db.map.Map
Row = db.row.Row

class Project(Operator):

    def __init__(self, input, input_rowtype, output_rowtype):
        Operator.__init__(self)
        self._input = input
        self._input_rowtype = input_rowtype
        self._output_rowtype = output_rowtype

    def open(self):
        self._input.open()

    def next(self):
        output_row = None
        input_row = None
        first = True
        while (input_row is not None and output_row is None) or first:
            input_row = self._input.next()
            if input_row:
                output_row = self.project_row(input_row)
            first = False
        return output_row

    def close(self):
        self._input.close()

    def stats(self):
        return self._input.stats()

    def project_row(self, input_row):
        output_row = None
        if input_row.rowtype is self._input_rowtype:
            output_row = self.row([(field, input_row[field])
                                   for field in self._output_rowtype.value])
        else:
            output_row = input_row
        return output_row

    def row(self, keyvals):
        assert len(keyvals) == len(self._output_rowtype.value)
        for k, v in keyvals:
            assert k in self._output_rowtype.value
        return Row(self._output_rowtype, dict(keyvals))
