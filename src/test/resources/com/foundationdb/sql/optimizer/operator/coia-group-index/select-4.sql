SELECT order_date,sku
FROM orders INNER JOIN items ON orders.oid = items.oid
WHERE sku < '8888'
ORDER BY sku,order_date
