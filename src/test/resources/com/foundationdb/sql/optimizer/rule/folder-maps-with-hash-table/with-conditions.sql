SELECT order_date, child.name FROM customers,orders,items,parent,child
 WHERE customers.cid = orders.cid
   AND orders.oid = items.oid
   AND parent.id = child.pid
   AND customers.name = parent.name
   AND order_date > '2011-01-01'
   AND sku = '1234'
   AND child.name <> 'Smith'
