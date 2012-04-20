SELECT name, order_date
  FROM customers 
 RIGHT JOIN orders ON customers.cid = orders.cid AND orders.order_date <> '2011-01-01'