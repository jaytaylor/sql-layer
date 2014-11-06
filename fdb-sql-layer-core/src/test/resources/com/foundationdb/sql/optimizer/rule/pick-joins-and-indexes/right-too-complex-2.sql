SELECT name, order_date
  FROM customers 
 RIGHT JOIN orders ON customers.cid = orders.cid AND customers.name <> 'Smith'