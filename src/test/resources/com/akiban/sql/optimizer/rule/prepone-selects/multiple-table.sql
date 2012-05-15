SELECT DISTINCT customers.cid
  FROM customers
 INNER JOIN addresses ON customers.cid = addresses.cid
 WHERE name = city