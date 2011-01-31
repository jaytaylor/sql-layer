#
# Copyright (C) 2011 Akiban Technologies Inc.
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License, version 3,
# as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see http://www.gnu.org/licenses.
#

import operator
import db.row

UnaryOperator = operator.UnaryOperator
Row = db.row.Row

class Project(UnaryOperator):

    def __init__(self, input, input_rowtype, output_rowtype):
        UnaryOperator.__init__(self, input)
        self._input_rowtype = input_rowtype
        self._output_rowtype = output_rowtype

    def handle_row(self, row):
        if row.rowtype is self._input_rowtype:
            output_row = self.row([(field, row[field])
                                   for field in self._output_rowtype.value])
        else:
            output_row = row
        return output_row

    def row(self, keyvals):
        assert len(keyvals) == len(self._output_rowtype.value)
        for k, v in keyvals:
            assert k in self._output_rowtype.value
        return Row(self._output_rowtype, dict(keyvals))
