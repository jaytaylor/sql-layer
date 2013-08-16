SELECT name, COUNT(*), COUNT(order_date), COUNT(DISTINCT order_date), COUNT(special) 
  FROM customers INNER JOIN orders ON customers.cid = orders.cid
GROUP BY name
