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

from schema import *
from db import *
from algebra import *

def run_plan(label, plan):
    print '--------------------------------------------------------------------------------'
    print '%s:' % label
    plan.open()
    row = plan.next()
    while row:
        print row
        row = plan.next()
    plan.close()
    stats = plan.stats()
    print ('random: %s\t\tsequential: %s\t\tsort: %s' %
           (stats[RANDOM_ACCESS], stats[SEQUENTIAL_ACCESS], stats[SORT]))

# Define user tables types

Tc = RowType('customer', ['hkey', 'cid', 'name'], ['hkey'])
To = RowType('order', ['hkey', 'oid', 'cid', 'order_date'], ['hkey'], parent_path = Tc.path)
Ti = RowType('item', ['hkey', 'iid', 'oid', 'unit_price', 'quantity'], ['hkey'], parent_path = To.path)

# Populate a group table

coi = Map()
coi.add(Row(Tc, {'hkey': [Tc, [4]], 'cid': 4, 'name': 'jack'}))
coi.add(Row(Tc, {'hkey': [Tc, [3]], 'cid': 3, 'name': 'tom'}))
coi.add(Row(Tc, {'hkey': [Tc, [2]], 'cid': 2, 'name': 'david'}))
coi.add(Row(Tc, {'hkey': [Tc, [1]], 'cid': 1, 'name': 'ori'}))
coi.add(Row(To, {'hkey': [Tc, [1], To, [11]], 'oid': 11, 'cid': 1, 'order_date': '2010/1/1'}))
coi.add(Row(To, {'hkey': [Tc, [1], To, [12]], 'oid': 12, 'cid': 1, 'order_date': '2010/1/2'}))
coi.add(Row(To, {'hkey': [Tc, [1], To, [13]], 'oid': 13, 'cid': 1, 'order_date': '2010/1/3'}))
coi.add(Row(To, {'hkey': [Tc, [2], To, [21]], 'oid': 21, 'cid': 2, 'order_date': '2010/2/1'}))
coi.add(Row(To, {'hkey': [Tc, [2], To, [22]], 'oid': 22, 'cid': 2, 'order_date': '2010/2/2'}))
coi.add(Row(To, {'hkey': [Tc, [2], To, [23]], 'oid': 23, 'cid': 2, 'order_date': '2010/2/3'}))
coi.add(Row(To, {'hkey': [Tc, [3], To, [31]], 'oid': 31, 'cid': 3, 'order_date': '2010/3/1'}))
coi.add(Row(To, {'hkey': [Tc, [3], To, [32]], 'oid': 32, 'cid': 3, 'order_date': '2010/3/2'}))
coi.add(Row(To, {'hkey': [Tc, [3], To, [33]], 'oid': 33, 'cid': 3, 'order_date': '2010/3/3'}))
coi.add(Row(To, {'hkey': [Tc, [-1], To, [34]], 'oid': 34, 'cid': -1, 'order_date': '2010/9/9'})) # orphan
coi.add(Row(Ti, {'hkey': [Tc, [1], To, [11], Ti, [111]], 'iid': 111, 'oid': 11, 'unit_price': 1.00, 'quantity': 1}))
coi.add(Row(Ti, {'hkey': [Tc, [1], To, [11], Ti, [112]], 'iid': 112, 'oid': 11, 'unit_price': 2.00, 'quantity': 2}))
coi.add(Row(Ti, {'hkey': [Tc, [1], To, [12], Ti, [121]], 'iid': 121, 'oid': 12, 'unit_price': 3.00, 'quantity': 3}))
coi.add(Row(Ti, {'hkey': [Tc, [1], To, [12], Ti, [122]], 'iid': 122, 'oid': 12, 'unit_price': 4.00, 'quantity': 4}))
coi.add(Row(Ti, {'hkey': [Tc, [1], To, [13], Ti, [131]], 'iid': 131, 'oid': 13, 'unit_price': 5.00, 'quantity': 5}))
coi.add(Row(Ti, {'hkey': [Tc, [1], To, [13], Ti, [132]], 'iid': 132, 'oid': 13, 'unit_price': 6.00, 'quantity': 6}))
coi.add(Row(Ti, {'hkey': [Tc, [2], To, [21], Ti, [211]], 'iid': 211, 'oid': 21, 'unit_price': 7.00, 'quantity': 7}))
coi.add(Row(Ti, {'hkey': [Tc, [2], To, [21], Ti, [212]], 'iid': 212, 'oid': 21, 'unit_price': 8.00, 'quantity': 8}))
coi.add(Row(Ti, {'hkey': [Tc, [2], To, [22], Ti, [221]], 'iid': 221, 'oid': 22, 'unit_price': 9.00, 'quantity': 9}))
coi.add(Row(Ti, {'hkey': [Tc, [2], To, [22], Ti, [222]], 'iid': 222, 'oid': 22, 'unit_price': 10.00, 'quantity': 10}))
coi.add(Row(Ti, {'hkey': [Tc, [2], To, [23], Ti, [231]], 'iid': 231, 'oid': 23, 'unit_price': 11.00, 'quantity': 11}))
coi.add(Row(Ti, {'hkey': [Tc, [2], To, [23], Ti, [232]], 'iid': 232, 'oid': 23, 'unit_price': 12.00, 'quantity': 12}))
coi.add(Row(Ti, {'hkey': [Tc, [3], To, [31], Ti, [311]], 'iid': 311, 'oid': 31, 'unit_price': 13.00, 'quantity': 13}))
coi.add(Row(Ti, {'hkey': [Tc, [3], To, [31], Ti, [312]], 'iid': 312, 'oid': 31, 'unit_price': 14.00, 'quantity': 14}))
coi.add(Row(Ti, {'hkey': [Tc, [3], To, [32], Ti, [321]], 'iid': 321, 'oid': 32, 'unit_price': 15.00, 'quantity': 15}))
coi.add(Row(Ti, {'hkey': [Tc, [3], To, [32], Ti, [322]], 'iid': 322, 'oid': 32, 'unit_price': 16.00, 'quantity': 16}))
coi.add(Row(Ti, {'hkey': [Tc, [3], To, [33], Ti, [331]], 'iid': 331, 'oid': 33, 'unit_price': 17.00, 'quantity': 17}))
coi.add(Row(Ti, {'hkey': [Tc, [3], To, [33], Ti, [332]], 'iid': 332, 'oid': 33, 'unit_price': 18.00, 'quantity': 18}))
coi.close()

# Add an index on customer.name

TIcname = RowType('customer.name', ['name', 'hkey'], ['name'])
customer_name_index = coi.add_index(Tc, TIcname)
