SELECT name,order_date 
  FROM customers
  LEFT OUTER JOIN orders ON customers.cid = orders.cid AND order_date > ?
