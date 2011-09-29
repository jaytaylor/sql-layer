SELECT name FROM customers
 WHERE 1 IN (SELECT 2 FROM orders WHERE customers.cid = orders.cid)