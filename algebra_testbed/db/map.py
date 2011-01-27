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

import schema.rowtype
import row

RowType = schema.rowtype.RowType
Row = row.Row

class Map(object):

    def __init__(self):
        self._extent = []

    def __iter__(self):
        return self._extent.__iter__()

    def add(self, row):
        self._extent.append(row)

    def close(self):
        self._extent.sort(key = lambda row: row.key)

    def add_index(self, rowtype, key_fields):
        return Index(self, rowtype, key_fields)

    def cursor(self, start = None, end = None):
        position = 0
        cursor = None
        if start is None:
            cursor = Cursor(self, 0, end)
        while not cursor and position < len(self._extent):
            row = self._extent[position]
            if row.key == start:
                cursor = Cursor(self, position, end)
            else:
                position += 1
        if not cursor:
            cursor = Cursor(self, position)
        return cursor

class Index(Map):

    def __init__(self, map, table_rowtype, index_rowtype):
        Map.__init__(self)
        for row in map:
            if row.rowtype is table_rowtype:
                index_value = {}
                for field in index_rowtype.value:
                    index_value[field] = row[field]
                self.add(Row(index_rowtype, index_value))
        self.close()

class Cursor(object):

    def __init__(self, map, position, end):
        self._map = map
        self._position = position
        self._end = end

    def next(self):
        extent = self._map._extent
        if self._position >= len(extent):
            next = None
        else:
            next = extent[self._position]
            if self._end is not None and next.key > self._end:
                next = None
                self._position = len(extent)
            else:
                self._position += 1
        return next
