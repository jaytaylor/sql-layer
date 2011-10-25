SELECT customers.name, order_date, child.name FROM customers,orders,parent,child
 WHERE customers.cid = orders.cid
   AND parent.id = child.pid
   AND customers.name = parent.name
   AND order_date > '2011-01-01'
   AND child.name <> 'Smith'
