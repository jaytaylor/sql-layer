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

run_plan("Restrict order date and customer name",
         Select
         (Select
          (Flatten
           (Flatten
            (FullTableScan(coi),
             Tc,
             To,
             Tco),
            Tco,
            Ti,
            Tcoi),
           Tcoi,
           lambda c: 'i' in c.name
           ),
          Tcoi,
          lambda o: '2010/1/2' <= o.order_date <= '2010/2/2'
          )
         )
