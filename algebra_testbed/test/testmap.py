from testbase import *

run_plan("Dump the entire group",
         FullTableScan(coi))

run_plan("Scan the group, selecting customers named tom",
         Select(FullTableScan(coi), Tc, lambda customer: customer.name == 'tom'))

run_plan("Scan the group, selecting orders made in January",
         Select(FullTableScan(coi), To, lambda order: order.order_date.startswith('2010/1')))

run_plan("Scan the customer name index",
         IndexScan(customer_name_index))

run_plan("Scan the customer name index for 'ori'",
         IndexScan(customer_name_index, ['ori']))

run_plan("Scan the customer name index for 'ori' and then find the row",
         IndexedTableScan(IndexScan(customer_name_index, ['ori']), ['hkey'], coi))

run_plan("Keep only orders (drop customers, items)",
         Extract(Cut(FullTableScan(coi), Ti), To))

Tco = RowType('co', ['hkey', 'cid', 'name', 'oid', 'order_date'], ['hkey'], To)

run_plan("Flatten customer and order",
         Flatten(FullTableScan(coi), Tc, To, Tco))

run_plan("Flatten customer and order, dropping customers with no orders",
         Flatten(FullTableScan(coi), Tc, To, Tco, INNER_JOIN))

run_plan("Flatten customer and order, keeping orders with no customers",
         Flatten(FullTableScan(coi), Tc, To, Tco, RIGHT_JOIN))

run_plan("Flatten customer and order and drop items",
         Cut(Flatten(FullTableScan(coi), Tc, To, Tco), Ti))

run_plan("Find the customer named 'jack' and flatten",
         Flatten(Select(FullTableScan(coi), Tc, lambda customer: customer.name == 'jack'), Tc, To, Tco))

run_plan("Find the customer named 'jack' and flatten, dropping jack if he has no orders (he doesn't)",
         Flatten(Select(FullTableScan(coi), Tc, lambda customer: customer.name == 'jack'),
                 Tc, To, Tco, INNER_JOIN))

Tcoi = RowType('coi',
               ['hkey', 'cid', 'name', 'oid', 'order_date', 'iid', 'oid', 'unit_price', 'quantity'],
               ['hkey'],
               Ti)

run_plan("Flatten everything using left join",
         Flatten(Flatten(FullTableScan(coi), Tc, To, Tco), Tco, Ti, Tcoi))

run_plan("Flatten everything using inner join",
         Flatten(Flatten(FullTableScan(coi), Tc, To, Tco, INNER_JOIN), Tco, Ti, Tcoi, INNER_JOIN))

run_plan("Flatten everything, keeping customers with no orders, and orders with no customers",
         Flatten(Flatten(FullTableScan(coi), Tc, To, Tco, LEFT_JOIN | RIGHT_JOIN), Tco, Ti, Tcoi))

run_plan("Sort customers by name",
         Sort(Cut(FullTableScan(coi), To), Tc, lambda customer: customer.name))

Tuq = RowType('uq', ['unit_price', 'quantity'])
Tq = RowType('q', ['quantity'])

run_plan("Project items twice",
         Project(
             Project(
                 Extract(FullTableScan(coi), Ti),
                 Ti, Tuq),
             Tuq, Tq))

run_plan("Select after Flatten",
         Select(
             Flatten(FullTableScan(coi), Tc, To, Tco),
             Tco,
             lambda co: co.name == 'tom'))

Tchn = RowType('chn', ['hkey', 'name'])
Tohd = RowType('ohd', ['hkey', 'order_date'])
Thnd = RowType('hnd', ['hkey', 'name', 'order_date'])
Tnd = RowType('nd', ['name', 'order_date'])

run_plan("select c.name, o.order_date from customer c, order o where o.cid = c.cid and o.order_date like '2010/1/%'",
         Project
         (Flatten
          (Project
           (Project
            (Select
             (Cut(FullTableScan(coi),
                  Ti),
              To,
              lambda order: order.order_date.startswith("2010/1/")),
             Tc,
             Tchn),
            To,
            Tohd),
           Tchn,
           Tohd,
           Thnd,
           INNER_JOIN),
          Thnd,
          Tnd))
