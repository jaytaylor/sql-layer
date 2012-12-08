SELECT name, order_date FROM customers
  LEFT JOIN orders ON customers.cid = orders.cid
  LEFT JOIN addresses ON addresses.cid = orders.cid
 WHERE state = 'MA'