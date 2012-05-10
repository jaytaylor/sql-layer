SELECT customers.cid
  FROM customers
 INNER JOIN orders ON customers.cid = orders.cid
 INNER JOIN items ON orders.oid = items.oid
 INNER JOIN addresses ON customers.cid = addresses.cid
 INNER JOIN parent ON parent.name = customers.name
 WHERE sku = '1234' AND quan > 100 AND parent.id > 1 
   AND parent.state = addresses.state AND addresses.city <> customers.name
