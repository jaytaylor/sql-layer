SELECT name, MIN(order_date), MAX(order_date) 
  FROM customers INNER JOIN orders on customers.cid = orders.cid
GROUP BY name