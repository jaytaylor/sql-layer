SELECT * FROM customers LEFT JOIN orders ON customers.cid = orders.cid
 WHERE orders.order_date > '2011-01-01'
