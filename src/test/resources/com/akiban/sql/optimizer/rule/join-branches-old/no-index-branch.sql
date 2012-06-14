SELECT name, state, order_date
  FROM customers 
 INNER JOIN addresses ON customers.cid = addresses.cid
 INNER JOIN orders ON customers.cid = orders.cid
