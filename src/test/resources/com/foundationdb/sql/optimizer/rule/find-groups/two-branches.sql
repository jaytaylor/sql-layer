SELECT * FROM customers,items,orders,addresses
 WHERE customers.cid = orders.cid
   AND items.oid = orders.oid
   AND customers.cid = addresses.cid