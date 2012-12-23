SELECT name, order_date FROM customers
  LEFT JOIN orders ON orders.cid = customers.cid
  LEFT JOIN addresses ON addresses.cid = customers.cid
 WHERE state = 'MA'