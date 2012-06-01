SELECT name, order_date
  FROM customers INNER JOIN orders ON customers.cid = orders.cid 
                 INNER JOIN addresses ON customers.cid = addresses.cid
 WHERE state = 'MA'
 ORDER BY order_date DESC
 LIMIT 10