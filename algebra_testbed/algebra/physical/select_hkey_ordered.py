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

import physicaloperator

SimpleOperator = physicaloperator.SimpleOperator

class Select(SimpleOperator):

    def __init__(self, input, predicate_rowtype, predicate):
        SimpleOperator.__init__(self, input)
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
