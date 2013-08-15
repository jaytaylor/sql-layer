SELECT order_date FROM orders LEFT JOIN items ON orders.oid = items.oid
 WHERE items.sku IS NULL
 ORDER BY order_date DESC