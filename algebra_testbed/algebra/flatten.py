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

Pending = operator.Pending
UnaryOperator = operator.UnaryOperator
Row = db.row.Row

KEEP_PARENT = 0x01
KEEP_CHILD =  0x02
INNER_JOIN =  0x04
LEFT_JOIN =   0x08
RIGHT_JOIN =  0x10
DEFAULT = LEFT_JOIN

class Flatten(UnaryOperator):

    def __init__(self, input, parent_type, child_type, flatten_type, flags = DEFAULT):
        UnaryOperator.__init__(self, input)
        self._parent_type = parent_type
        self._child_type = child_type
        self._parent = None
        self._child = None
        self._childless_parent = None
        self._rowtype = flatten_type
        self._fields_from_parent = []
        self._fields_in_child_only = []
        for field in self._parent_type.value:
            if field not in child_type.value:
                self._fields_from_parent.append(field)
        for field in self._child_type.value:
            if field not in self._parent_type.value:
                self._fields_in_child_only.append(field)
        self._keep_parent = (flags & KEEP_PARENT) != 0
        self._keep_child = (flags & KEEP_CHILD) != 0
        self._left_join = (flags & LEFT_JOIN) != 0
        self._right_join = (flags & RIGHT_JOIN) != 0
        self._pending = Pending()

    def next(self):
        output_row = self.pending()
        if output_row is None:
            output_row = UnaryOperator.next(self)
        # child rows are processed immediately. parent rows are not, 
        # because when seen, we don't know if the next row will be another
        # parent, a child, a row of child type that is not actually a child,
        # a row of type other than parent or child, or end of stream. If we get
        # here and output_row is None, then there could be a childless parent
        # waiting to be processed.
        if output_row is None:
            if self._parent:
                self.left_join_row()
                output_row = self.pending()
            self._parent = None
            self._child = None
            self._childless_parent = None
        return output_row
        

    def handle_row(self, row):
        if row.rowtype is self._parent_type:
            if self._keep_parent:
                self._pending.add(row)
            if self._parent:
                self.left_join_row()
            self._parent = row
            self._child = None
            self._childless_parent = True
        elif row.rowtype is self._child_type:
            if self._keep_child:
                self._pending.add(row)
            self._child = row
            if self._parent and self._parent.ancestor_of(self._child):
                # child is not an orphan
                self._childless_parent = False
                self.inner_join_row()
            else:
                # child is an orphan
                self._parent = None
                self._childless_parent = None
                self.right_join_row()
        else:
            self._pending.add(row)
            if self._parent_type.ancestor_of(row.rowtype):
                if self._parent and not self._parent.ancestor_of(row):
                    self._parent = None
                    self._childless_parent = None
                if (self._child_type.ancestor_of(row.rowtype) and
                    self._child and
                    not self._child.ancestor_of(row)):
                    self._child = None
        return self.pending()

    def pending(self):
        return self._pending.take()

    def inner_join_row(self):
        flattened = {}
        assert self._parent
        assert self._child
        for field in self._child_type.value:
            flattened[field] = self._child[field]
        for field in self._fields_from_parent:
            flattened[field] = self._parent[field]
        self._pending.add(Row(self._rowtype, flattened))

    def left_join_row(self):
        assert self._parent
        if self._left_join and self._child is None:
            flattened = {}
            for field in self._parent_type.value:
                flattened[field] = self._parent[field]
            for field in self._fields_in_child_only:
                flattened[field] = None
            self._pending.add(Row(self._rowtype, flattened))

    def right_join_row(self):
        assert self._parent is None
        assert self._child
        if self._right_join:
            flattened = {}
            for field in self._parent_type.value:
                flattened[field] = None
            for field in self._child_type.value:
                flattened[field] = self._child[field]
            self._pending.add(Row(self._rowtype, flattened))
