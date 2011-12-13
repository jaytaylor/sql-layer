SELECT DISTINCT name, order_date
  FROM customers
 INNER JOIN orders ON customers.cid = orders.cid
 INNER JOIN items ON orders.oid = items.oid
 ORDER BY name
 LIMIT 2