SELECT customers.name,order_date,sku,quan
FROM customers
INNER JOIN orders ON customers.cid = orders.cid
RIGHT JOIN items ON orders.oid = items.oid
ORDER BY sku, order_date
