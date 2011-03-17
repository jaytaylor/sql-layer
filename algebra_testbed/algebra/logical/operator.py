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

class Operator(object):

    def __init__(self, input):
        self._input = input

    input = property(lambda self: self._input)

    def implementation(self, input):
        assert False, 'Must be provided by logical operator class'

    def generate_plan(self):
        if self._input is None:
            input = None
        else:
            input = self._input.generate_plan()
        return self.implementation(input)
