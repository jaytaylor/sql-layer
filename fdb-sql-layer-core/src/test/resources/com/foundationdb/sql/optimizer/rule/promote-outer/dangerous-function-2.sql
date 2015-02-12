SELECT * FROM customers LEFT JOIN orders ON customers.cid = orders.cid
 WHERE IFNULL(orders.order_date, CURRENT DATE) > '2011-01-01'
