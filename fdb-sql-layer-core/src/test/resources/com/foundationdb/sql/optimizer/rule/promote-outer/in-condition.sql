SELECT * FROM customers LEFT JOIN orders ON customers.cid = orders.cid
 WHERE orders.order_date IN ('2010-12-31', '2011-01-01')
