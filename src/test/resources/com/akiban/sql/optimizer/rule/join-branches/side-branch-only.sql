SELECT order_date
  FROM customers 
  INNER JOIN addresses on customers.cid = addresses.cid
  INNER JOIN orders ON customers.cid = orders.cid
 WHERE customers.name = 'Jones' AND addresses.state = 'MA'
