SELECT orders.oid, items.quan
  FROM orders INNER JOIN items ON orders.oid = items.oid
 WHERE items.sku = '1234'
 GROUP BY 1
 ORDER BY order_date DESC
