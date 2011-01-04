import operator
import db.map
import db.row

Operator = operator.Operator
Map = db.map.Map
Row = db.row.Row

KEEP_PARENT = 0x01
KEEP_CHILD =  0x02
INNER_JOIN =  0x04
LEFT_JOIN =   0x08
RIGHT_JOIN =  0x10
DEFAULT = LEFT_JOIN

class _Node(object):

    def __init__(self, object):
        self.object = object
        self.next = None

class _Pending(object):

    def __init__(self):
        self.head = None
        self.tail = None

    def add(self, object):
        node = _Node(object)
        if self.head is None:
            self.head = node
            self.tail = node
        else:
            self.tail.next = node
            self.tail = node

    def take(self):
        object = None
        if self.head:
            object = self.head.object
            self.head = self.head.next
            if self.head is None:
                self.tail = None
        return object

class Flatten(Operator):

    def __init__(self, input, parent_type, child_type, flatten_type, flags = DEFAULT):
        Operator.__init__(self)
        self._input = input
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
        self._pending = _Pending()

    def open(self):
        self._input.open()

    def next(self):
        output_row = self.pending()
        if output_row is None:
            input_row = self._input.next()
            while output_row is None and input_row is not None:
                if input_row.rowtype is self._parent_type:
                    if self._keep_parent:
                        self._pending.add(input_row)
                    if self._parent:
                        self.left_join_row()
                    self._parent = input_row
                    self._child = None
                    self._childless_parent = True
                elif input_row.rowtype is self._child_type:
                    if self._keep_child:
                        self._pending.add(input_row)
                    self._child = input_row
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
                    self._pending.add(input_row)
                    if self._parent_type.ancestor_of(input_row.rowtype):
                        if self._parent and not self._parent.ancestor_of(input_row):
                            self._parent = None
                            self._childless_parent = None
                        if (self._child_type.ancestor_of(input_row.rowtype) and
                            self._child and
                            not self._child.ancestor_of(input_row)):
                            self._child = None
                output_row = self.pending()
                if output_row is None:
                    input_row = self._input.next()
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

    def close(self):
        self._input.close()

    def stats(self):
        return self._input.stats()

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
        row = None
        assert self._parent
        if self._left_join and self._child is None:
            flattened = {}
            for field in self._parent_type.value:
                flattened[field] = self._parent[field]
            for field in self._fields_in_child_only:
                flattened[field] = None
            row = Row(self._rowtype, flattened)
        if row:
            self._pending.add(row)

    def right_join_row(self):
        row = None
        assert self._parent is None
        assert self._child
        if self._right_join:
            flattened = {}
            for field in self._parent_type.value:
                flattened[field] = None
            for field in self._child_type.value:
                flattened[field] = self._child[field]
            row = Row(self._rowtype, flattened)
        if row:
            self._pending.add(row)
