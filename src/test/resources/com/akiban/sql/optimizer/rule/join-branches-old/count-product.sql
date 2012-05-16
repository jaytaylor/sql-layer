SELECT COUNT(*)
FROM orders
 INNER JOIN customers ON customers.cid = orders.cid
 INNER JOIN addresses on customers.cid = addresses.cid
 WHERE customers.name = 'Smith'
