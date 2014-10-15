SELECT * FROM customers LEFT JOIN orders ON customers.cid = orders.cid
 WHERE CASE WHEN name = 'Smith' THEN (order_date > '2010-01-01') ELSE (order_date > '2011-01-01') END
