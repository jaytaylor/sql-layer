SELECT MAX(items.sku)
  FROM customers, orders, items, addresses
 WHERE customers.cid = orders.cid
   AND orders.oid = items.oid
   AND customers.cid = addresses.cid
   AND customers.name IN ('Smith', 'Jones')
