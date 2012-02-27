SELECT order_date, child.name FROM customers,orders,parent,child
 WHERE customers.cid = orders.cid
   AND parent.name = child.name
   AND customers.name = parent.name
