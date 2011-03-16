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

Operator = operator.Operator

# Reference does not include group_key parameter. Included here
# because testbed does not model hkeys accurately.

_DONE = object()

class IndexLookup(Operator):

    def __init__(self, input, group_key, group):
        Operator.__init__(self)
        self._input = input
        self._group_key = group_key
        self._group = group
        self._cursor = None
        self._index_row = None

    def open(self):
        self._input.open()

    def next(self):
        group_row = None
        while group_row is None and self._index_row is not _DONE:
            if self._index_row is None:
                self._index_row = self._input.next()
            if self._index_row is None:
                self._index_row = _DONE
            else:
                if not self._cursor:
                    key = [self._index_row[field]
                           for field in self._group_key]
                    self._cursor = self._group.cursor(key, None)
                    self.count_random_access()
                group_row = self._cursor.next()
                self.count_sequential_access()
                if group_row:
                    if not self._index_row.ancestor_of(group_row):
                        group_row = None
                        self._index_row = None
                        self._cursor = None
                else:
                    self._index_row = None
                    self._cursor = None
        return group_row

    def close(self):
        self._input.close()

    def stats(self):
        return self._stats.merge(self._input.stats())
