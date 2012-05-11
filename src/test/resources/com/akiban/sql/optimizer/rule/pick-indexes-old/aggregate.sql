SELECT name, MIN(order_date)
  FROM customers INNER JOIN orders ON customers.cid = orders.cid
 GROUP BY name
