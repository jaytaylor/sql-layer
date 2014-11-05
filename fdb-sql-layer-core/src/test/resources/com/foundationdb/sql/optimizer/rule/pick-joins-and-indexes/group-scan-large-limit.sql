SELECT order_date, sku, quan 
  FROM orders INNER JOIN items ON orders.oid = items.oid
 LIMIT 30000