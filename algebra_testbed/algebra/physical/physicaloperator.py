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

N_STATS = 3
RANDOM_ACCESS = 0
SEQUENTIAL_ACCESS = 1
SORT = 2

class Stats(object):

    def __init__(self):
        self._stats = [0] * N_STATS

    def __getitem__(self, i):
        return self._stats[i]

    def __setitem__(self, i, x):
        self._stats[i] = x

    def merge(self, other):
        merged = Stats()
        for i in xrange(N_STATS):
            merged[i] = self[i] + other[i]
        return merged

class PhysicalOperator(object):

    def __init__(self, input):
        self._stats = Stats()
        self._input = input

    def open(self):
        assert False

    def next(self):
        assert False

    input = property(lambda self: self._input)

    def close(self):
        assert False

    def stats(self):
        assert False

    def count_random_access(self):
        self._stats[RANDOM_ACCESS] += 1

    def count_sequential_access(self):
        self._stats[SEQUENTIAL_ACCESS] += 1

    def count_sort(self, n):
        self._stats[SORT] += n

# Boilerplate for a common pattern: Read from input until a row is
# found, then handle that row.

class SimpleOperator(PhysicalOperator):

    def __init__(self, input):
        PhysicalOperator.__init__(self, input)
        self._input = input

    def open(self):
        self._input.open()

    def next(self):
        output_row = None
        input_row = self._input.next()
        while input_row is not None and output_row is None:
            output_row = self.handle_row(input_row)
            if output_row is None:
                input_row = self._input.next()
        return output_row

    def close(self):
        self._input.close()

    def stats(self):
        return self._stats.merge(self._input.stats())

    def handle_row(self, row):
        assert False
