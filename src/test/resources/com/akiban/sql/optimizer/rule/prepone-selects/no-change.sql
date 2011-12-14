SELECT *
 FROM customers INNER JOIN orders ON customers.cid = orders.cid
 WHERE UPPER(customers.name) = orders.special