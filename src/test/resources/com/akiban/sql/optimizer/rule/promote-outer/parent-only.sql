SELECT * FROM customers LEFT JOIN orders ON customers.cid = orders.cid
 WHERE customers.name = 'Smith'
