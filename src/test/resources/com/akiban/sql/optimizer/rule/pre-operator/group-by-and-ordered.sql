SELECT orders.oid, order_date
  FROM orders INNER JOIN items ON orders.oid = items.oid
 WHERE items.sku = '1234'
 GROUP BY order_date, oid
 ORDER BY order_date DESC
