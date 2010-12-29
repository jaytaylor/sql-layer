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
coi.add(Row(Ti, {'hkey': [Tc, [1], To, [11], Ti, [111]], 'iid': 111, 'oid': 11, 'unit_price': 10.00, 'quantity': 3}))
coi.add(Row(Ti, {'hkey': [Tc, [1], To, [11], Ti, [112]], 'iid': 112, 'oid': 11, 'unit_price': 10.00, 'quantity': 3}))
coi.add(Row(Ti, {'hkey': [Tc, [1], To, [12], Ti, [121]], 'iid': 121, 'oid': 12, 'unit_price': 10.00, 'quantity': 3}))
coi.add(Row(Ti, {'hkey': [Tc, [1], To, [12], Ti, [122]], 'iid': 122, 'oid': 12, 'unit_price': 10.00, 'quantity': 3}))
coi.add(Row(Ti, {'hkey': [Tc, [1], To, [13], Ti, [131]], 'iid': 131, 'oid': 13, 'unit_price': 10.00, 'quantity': 3}))
coi.add(Row(Ti, {'hkey': [Tc, [1], To, [13], Ti, [132]], 'iid': 132, 'oid': 13, 'unit_price': 10.00, 'quantity': 3}))
coi.add(Row(Ti, {'hkey': [Tc, [2], To, [21], Ti, [211]], 'iid': 211, 'oid': 21, 'unit_price': 10.00, 'quantity': 3}))
coi.add(Row(Ti, {'hkey': [Tc, [2], To, [21], Ti, [212]], 'iid': 212, 'oid': 21, 'unit_price': 10.00, 'quantity': 3}))
coi.add(Row(Ti, {'hkey': [Tc, [2], To, [22], Ti, [221]], 'iid': 221, 'oid': 22, 'unit_price': 10.00, 'quantity': 3}))
coi.add(Row(Ti, {'hkey': [Tc, [2], To, [22], Ti, [222]], 'iid': 222, 'oid': 22, 'unit_price': 10.00, 'quantity': 3}))
coi.add(Row(Ti, {'hkey': [Tc, [2], To, [23], Ti, [231]], 'iid': 231, 'oid': 23, 'unit_price': 10.00, 'quantity': 3}))
coi.add(Row(Ti, {'hkey': [Tc, [2], To, [23], Ti, [232]], 'iid': 232, 'oid': 23, 'unit_price': 10.00, 'quantity': 3}))
coi.add(Row(Ti, {'hkey': [Tc, [3], To, [31], Ti, [311]], 'iid': 311, 'oid': 31, 'unit_price': 10.00, 'quantity': 3}))
coi.add(Row(Ti, {'hkey': [Tc, [3], To, [31], Ti, [312]], 'iid': 312, 'oid': 31, 'unit_price': 10.00, 'quantity': 3}))
coi.add(Row(Ti, {'hkey': [Tc, [3], To, [32], Ti, [321]], 'iid': 321, 'oid': 32, 'unit_price': 10.00, 'quantity': 3}))
coi.add(Row(Ti, {'hkey': [Tc, [3], To, [32], Ti, [322]], 'iid': 322, 'oid': 32, 'unit_price': 10.00, 'quantity': 3}))
coi.add(Row(Ti, {'hkey': [Tc, [3], To, [33], Ti, [331]], 'iid': 331, 'oid': 33, 'unit_price': 10.00, 'quantity': 3}))
coi.add(Row(Ti, {'hkey': [Tc, [3], To, [33], Ti, [332]], 'iid': 332, 'oid': 33, 'unit_price': 10.00, 'quantity': 3}))
coi.close()

# Add an index on customer.name

TIcname = RowType('customer.name', ['name', 'hkey'], ['name'])
customer_name_index = coi.add_index(Tc, TIcname)

# Run some queries

run_plan("Dump the entire group",
         TableScan(coi))

run_plan("Scan the group, selecting customers named tom",
         Select(TableScan(coi), Tc, lambda customer: customer.name == 'tom'))

run_plan("Scan the group, selecting orders made in January",
         Select(TableScan(coi), To, lambda order: order.order_date.startswith('2010/1')))

run_plan("Scan the customer name index",
         IndexScan(customer_name_index))

run_plan("Scan the customer name index for 'ori'",
         IndexScan(customer_name_index, ['ori']))

run_plan("Scan the customer name index for 'ori' and then find the row",
         TableFind(IndexScan(customer_name_index, ['ori']), ['hkey'], coi))

run_plan("Keep only orders (drop customers, items)",
         Extract(Cut(TableScan(coi), Ti), To))

Tco = RowType('co', ['hkey', 'cid', 'name', 'oid', 'order_date'], ['hkey'], To)

run_plan("Flatten customer and order",
         Flatten(TableScan(coi), Tc, To, Tco))

run_plan("Flatten customer and order, dropping customers with no orders",
         Flatten(TableScan(coi), Tc, To, Tco, INNER_JOIN))

run_plan("Flatten customer and order, keeping orders with no customers",
         Flatten(TableScan(coi), Tc, To, Tco, RIGHT_JOIN))

run_plan("Flatten customer and order and drop items",
         Cut(Flatten(TableScan(coi), Tc, To, Tco), Ti))

run_plan("Find the customer named 'jack' and flatten",
         Flatten(Select(TableScan(coi), Tc, lambda customer: customer.name == 'jack'), Tc, To, Tco))

run_plan("Find the customer named 'jack' and flatten, dropping jack if he has no orders (he doesn't)",
         Flatten(Select(TableScan(coi), Tc, lambda customer: customer.name == 'jack'),
                 Tc, To, Tco, INNER_JOIN))

Tcoi = RowType('coi',
               ['hkey', 'cid', 'name', 'oid', 'order_date', 'iid', 'oid', 'unit_price', 'quantity'],
               ['hkey'],
               Ti)

run_plan("Flatten everything using left join",
         Flatten(Flatten(TableScan(coi), Tc, To, Tco), Tco, Ti, Tcoi))

run_plan("Flatten everything using inner join",
         Flatten(Flatten(TableScan(coi), Tc, To, Tco, INNER_JOIN), Tco, Ti, Tcoi, INNER_JOIN))

run_plan("Flatten everything, keeping customers with no orders, and orders with no customers",
         Flatten(Flatten(TableScan(coi), Tc, To, Tco, LEFT_JOIN | RIGHT_JOIN), Tco, Ti, Tcoi))

run_plan("Sort customers by name",
         Sort(Cut(TableScan(coi), To), Tc, lambda customer: customer.name))

Tuq = RowType('uq', ['unit_price', 'quantity'])
Tq = RowType('q', ['quantity'])

run_plan("Project items twice",
         Project(
             Project(
                 Extract(TableScan(coi), Ti),
                 Ti, Tuq),
             Tuq, Tq))

run_plan("Select after Flatten",
         Select(
             Flatten(TableScan(coi), Tc, To, Tco),
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
             (Cut(TableScan(coi),
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
