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

PhysicalOperator = physicaloperator.PhysicalOperator

class Scan(PhysicalOperator):

    def __init__(self, cursor):
        PhysicalOperator.__init__(self, None)
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
