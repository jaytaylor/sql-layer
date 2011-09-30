SELECT orders.order_date FROM customers INNER JOIN orders ON customers.cid = orders.cid LEFT JOIN items ON orders.oid = items.oid ORDER BY customers.name, items.sku
