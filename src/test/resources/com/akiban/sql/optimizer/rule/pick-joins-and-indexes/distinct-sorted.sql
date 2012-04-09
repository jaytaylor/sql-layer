SELECT DISTINCT customers.cid, name FROM customers, orders
 WHERE customers.cid = orders.cid 
 ORDER BY name