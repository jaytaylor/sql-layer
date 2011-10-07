SELECT '[[['||name||']]]', order_date FROM customers
 INNER JOIN orders on customers.cid = orders.cid
 WHERE order_date > '2011-01-01'
 ORDER BY 1
