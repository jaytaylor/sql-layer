SELECT 1, 'hello' FROM orders INNER JOIN items ON orders.oid = items.oid WHERE sku = '1234' AND order_date > '2011-01-01'
