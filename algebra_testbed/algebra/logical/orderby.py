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

class OrderBy(operator.Operator):

    def __init__(self, input, rowtype, sort_key):
        operator.Operator.__init__(self, input)
        self._rowtype = rowtype
        self._sort_key = sort_key

    def implementation(self, input):
        return sort.Sort(input, self._rowtype, self._sort_key)
