SELECT order_date, child.name FROM customers,orders,parent,child
 WHERE customers.cid = orders.cid
   AND parent.id = child.pid
   AND customers.name = parent.name
