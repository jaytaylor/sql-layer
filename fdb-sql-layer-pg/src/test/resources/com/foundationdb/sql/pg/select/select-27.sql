SELECT name, city
  FROM customers
 INNER JOIN orders ON customers.cid = orders.cid INNER JOIN items ON orders.oid = items.oid
 LEFT JOIN addresses ON customers.cid = addresses.cid
 WHERE sku = '9876'
