SELECT state FROM customers RIGHT JOIN addresses ON customers.cid = addresses.cid
 WHERE customers.name IS NULL
 ORDER BY state DESC