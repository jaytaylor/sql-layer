SELECT MAX(items.sku)
  FROM customers, orders, items
 WHERE customers.cid = orders.cid
   AND orders.oid = items.oid
   AND customers.name IN ('Smith', 'Jones', 'Adams')
