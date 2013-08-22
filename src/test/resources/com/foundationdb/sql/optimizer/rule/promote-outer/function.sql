SELECT * FROM customers LEFT JOIN orders ON customers.cid = orders.cid
 WHERE MONTH(orders.order_date) > 6
