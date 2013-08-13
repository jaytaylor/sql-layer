SELECT name FROM customers, orders, items 
 WHERE customers.cid = orders.cid
   AND orders.oid = items.oid
   AND orders.order_date = '2011-03-01'
