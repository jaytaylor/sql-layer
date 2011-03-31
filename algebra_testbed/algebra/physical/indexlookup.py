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

import collections
import physicaloperator

PhysicalOperator = physicaloperator.PhysicalOperator

# Reference does not include group_key parameter. Included here
# because testbed does not model hkeys accurately.

class IndexLookup(PhysicalOperator):

    # missing_types: Types of the group above the rowtype associated with the index.
    # E.g., an index on order.order_date yields order and item rows. missing_types
    # would then be [customer].
    # TODO: missing_types processing should accomodate types other than ancestors, e.g.
    # address (parent is customer, so it's a sibling of order).
    def __init__(self, input, group_key, group, missing_types):
        PhysicalOperator.__init__(self, input)
        self._input = input
        self._group_key = group_key
        self._group = group
        self._missing_types = missing_types
        self._group_cursor = None
        self._index_row = None
        self._pending = collections.deque()

    def open(self):
        self._input.open()
        self.advance_index()

    def next(self):
        group_row = None
        while group_row is None and self._index_row is not None:
            group_row = self.pending()
            if group_row is None:
                group_row = self._group_cursor.next()
                if group_row is None:
                    self.advance_index()
            if group_row and not self._index_row.ancestor_of(group_row):
                group_row = None
        return group_row

    def pending(self):
        try:
            return self._pending.popleft()
        except IndexError:
            return None

    def close(self):
        self._input.close()

    def stats(self):
        return self._stats.merge(self._input.stats())

    def advance_index(self):
        # Retrieve rows based on hkey from index. The types of these rows
        # will be the hkey's rowtype and descendent types. The cursor actually
        # scans to the end, but later there is a check that the index row is
        # an ancestor of a row retrieved from the cursor.
        self._index_row = self._input.next()
        self.count_sequential_access()
        if self._index_row is not None:
            key = [self._index_row[field] for field in self._group_key]
            self._group_cursor = self._group.cursor(key, None)
            self.count_random_access()
            self.find_ancestors(key)

    def find_ancestors(self, key):
        # Get rows of missing types. Try hkeys of length 2, 4, ... len(key) - 2. 
        # (Steps of 2 because hkey structure is [ordinal, [key values], ... ]
        hkey = key[0]
        hkey_prefix_length = 2
        while hkey_prefix_length < len(hkey):
            ancestor_hkey = hkey[:hkey_prefix_length]
            ancestor_cursor = self._group.cursor([ancestor_hkey], [ancestor_hkey])
            ancestor_row = ancestor_cursor.next()
            self.count_random_access()
            if ancestor_row is not None and ancestor_row.rowtype in self._missing_types:
                self._pending.append(ancestor_row)
            hkey_prefix_length += 2
