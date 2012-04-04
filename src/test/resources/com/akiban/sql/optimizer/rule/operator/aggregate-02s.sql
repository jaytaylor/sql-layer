SELECT name, COUNT(*)
  FROM customers INNER JOIN orders on customers.cid = orders.cid
GROUP BY name
ORDER BY name
