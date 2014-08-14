SELECT sku FROM customers
 INNER JOIN orders ON customers.cid = orders.cid
 INNER JOIN items ON orders.oid = items.oid
  WHERE name IN ('Smith', 'Jones', 'Adams')
  ORDER BY sku DESC