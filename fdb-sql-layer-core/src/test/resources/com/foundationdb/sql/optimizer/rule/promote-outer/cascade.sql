SELECT name, order_date FROM customers
  LEFT JOIN orders ON orders.cid = customers.cid
  LEFT JOIN addresses ON addresses.cid = orders.cid
 WHERE state = 'MA'