SELECT * FROM customers LEFT JOIN orders ON customers.cid = orders.cid
 WHERE orders.order_date < '2000-01-01' OR orders.order_date > '2011-12-31'
