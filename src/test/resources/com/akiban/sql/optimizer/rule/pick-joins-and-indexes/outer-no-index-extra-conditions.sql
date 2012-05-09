SELECT order_date FROM orders LEFT JOIN items ON orders.oid = items.oid AND items.quan > 0
 WHERE items.sku IS NULL
 ORDER BY order_date DESC