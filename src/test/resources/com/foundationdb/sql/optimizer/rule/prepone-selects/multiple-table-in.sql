SELECT customers.cid
  FROM customers
 INNER JOIN orders ON customers.cid = orders.cid
 INNER JOIN items ON orders.oid = items.oid
 WHERE name <> sku
   AND name IN (SELECT name FROM child WHERE pid = 1)
