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

from exceptions import Exception

class RowType(object):

    _rowtype_names = []

    def __init__(self, name, value, key = [], parent_path = None, path = None):
        if name in RowType._rowtype_names:
            raise Exception('Duplicate RowType name %s' % name)
        if parent_path is not None and path is not None:
            raise Exception("%s: Don't specify both parent_path and path" % name)
        RowType._rowtype_names.append(name)
        self._name = name
        self._value_fields = value
        self._key_fields = key
        if path is not None:
            self._path = path
        elif parent_path is not None:
            # Convenient shorthand: If parent_path is a rowtype, then use that
            # rowtype's path
            if isinstance(parent_path, RowType):
                rowtype = parent_path
                self._path = rowtype.path
            else:
                self._path = parent_path + [self]
        else:
            self._path = [self]

    def __repr__(self):
        return self._name

    name = property(lambda self: self._name)

    key = property(lambda self: self._key_fields)

    value = property(lambda self: self._value_fields)

    path = property(lambda self: self._path)

    def ancestor_of(self, other):
        self_path = self._path
        other_path = other._path
        ancestor = False
        if (self_path is not None and
            other_path is not None and
            len(self_path) <= len(other_path)):
            ancestor = self_path == other_path[:len(self_path)]
        return ancestor
