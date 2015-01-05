SELECT * FROM customers LEFT JOIN orders ON customers.cid = orders.cid
 WHERE CASE WHEN order_date < '2000-01-01' THEN (name = 'Smith') ELSE (name = 'Jones') END
