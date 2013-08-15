SELECT customers.name,order_date,sku,quan
FROM customers
INNER JOIN orders ON customers.cid = orders.cid
INNER JOIN items ON orders.oid = items.oid
ORDER BY sku DESC, order_date ASC
