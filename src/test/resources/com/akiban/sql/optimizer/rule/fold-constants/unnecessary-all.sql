SELECT name FROM customers
 WHERE 1 < ALL (SELECT 2 FROM orders WHERE customers.cid = orders.cid)