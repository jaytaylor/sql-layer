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

class Project(operator.Operator):

    def __init__(self, input, input_rowtype, output_rowtype):
        operator.Operator.__init__(self, input)
        self._input_rowtype = input_rowtype
        self._output_rowtype = output_rowtype

    def implementation(self, input):
        return project_default.Project(input, self._input_rowtype, self._output_rowtype)
