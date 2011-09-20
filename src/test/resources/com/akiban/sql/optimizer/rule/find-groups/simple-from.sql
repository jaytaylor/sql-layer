SELECT * FROM customers,items,orders
 WHERE customers.cid = orders.cid
   AND items.oid = orders.oid
