SELECT name, sku, SUM(QUAN)
  FROM customers, orders, items
 WHERE customers.cid = orders.cid
   AND orders.oid = items.oid
 GROUP BY name, sku
 ORDER BY name
