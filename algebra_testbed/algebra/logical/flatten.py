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
from algebra.physical import *

KEEP_PARENT = 0x01
KEEP_CHILD =  0x02
INNER_JOIN =  0x04
LEFT_JOIN =   0x08
RIGHT_JOIN =  0x10
DEFAULT = LEFT_JOIN

class Flatten(operator.Operator):

    def __init__(self, input, parent_type, child_type, flatten_type, flags = DEFAULT):
        operator.Operator.__init__(self, input)
        self._parent_type = parent_type
        self._child_type = child_type
        self._flatten_type = flatten_type
        self._flags = flags

    def implementation(self, input):
        return flatten_hkey_ordered.Flatten(input,
                                            self._parent_type,
                                            self._child_type,
                                            self._flatten_type,
                                            self._flags)
