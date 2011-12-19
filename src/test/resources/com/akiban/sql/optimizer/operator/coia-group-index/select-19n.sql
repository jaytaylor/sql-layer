SELECT MIN(items.sku)
  FROM customers, orders, items
 WHERE customers.cid = orders.cid
   AND orders.oid = items.oid

