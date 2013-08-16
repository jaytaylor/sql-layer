SELECT customers.cid
  FROM customers
 INNER JOIN orders ON customers.cid = orders.cid
 INNER JOIN items ON orders.oid = items.oid
 WHERE sku = '1234' AND quan > 100 
   AND name IN (SELECT name FROM child WHERE pid = 1)
