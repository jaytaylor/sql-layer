SELECT name, order_date
  FROM customers 
  INNER JOIN addresses on customers.cid = addresses.cid
  LEFT JOIN orders ON customers.cid = orders.cid
 WHERE addresses.state = 'MA'
 ORDER BY 1,2
