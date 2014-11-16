SELECT name, COUNT(*) FROM customers, orders
WHERE customers.cid = orders.cid
GROUP BY 1
