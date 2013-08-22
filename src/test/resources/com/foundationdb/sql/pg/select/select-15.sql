SELECT name, MIN(order_date), MAX(order_date), COUNT(*)
  FROM customers INNER JOIN orders ON customers.cid = orders.cid
 GROUP BY name
 ORDER BY name
