SELECT * FROM customers LEFT JOIN orders ON customers.cid = orders.cid
 WHERE YEAR(orders.order_date) > 2000
