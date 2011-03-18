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

from testbase import *

GroupScan = groupscan_default.GroupScan
Select = select_hkey_ordered.Select
IndexScan = indexscan.IndexScan
IndexLookup = indexlookup.IndexLookup
Extract = extract_default.Extract
Cut = cut_default.Cut
Flatten = flatten_hkey_ordered.Flatten
OrderBy = sort.Sort
Project = project_default.Project

"""
select c.name, o.date, i.*
from customer c, order o, item i
where o.date between '2010/1/2' and 2010/2/2'
and c.name like '%i%'
"""

Tco = RowType('co', ['hkey', 'cid', 'name', 'oid', 'order_date'], ['hkey'], To)
Tcoi = RowType('coi',
               ['hkey', 'cid', 'name', 'oid', 'order_date', 'iid', 'oid', 'unit_price', 'quantity'],
               ['hkey'],
               Ti)

run_physical_plan("Restrict order date and customer name",
         Select
         (Select
          (Flatten
           (Flatten
            (GroupScan(coi),
             Tc,
             To,
             Tco),
            Tco,
            Ti,
            Tcoi),
           Tcoi,
           lambda c: 'i' in c.name),
          Tcoi,
          lambda o: '2010/1/2' <= o.order_date <= '2010/2/2'))

run_physical_plan("Push down select on order",
         Select
         (Flatten
          (Flatten
           (Select
            (GroupScan(coi),
             To,
             lambda o: '2010/1/2' <= o.order_date <= '2010/2/2'),
            Tc,
            To,
            Tco),
           Tco,
           Ti,
           Tcoi),
          Tcoi,
          lambda c: 'i' in c.name))

run_physical_plan("Replace group scan by index scan",
         Select
         (Flatten
          (Flatten
           (IndexLookup
            (IndexScan(order_date_index, ['2010/1/2'], ['2010/2/2']),
             ['hkey'],
             coi,
             [Tc]),
            Tc,
            To,
            Tco),
           Tco,
           Ti,
           Tcoi),
          Tcoi,
          lambda c: 'i' in c.name))
