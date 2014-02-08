SELECT * FROM orders LEFT JOIN customers ON customers.cid = orders.cid INNER JOIN items ON orders.oid = items.oid
