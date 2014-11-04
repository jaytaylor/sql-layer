SELECT name, order_date
  FROM customers INNER JOIN orders ON customers.cid = orders.cid
 WHERE (1=1 AND name = 'Jones') OR (NOT(1=1) AND name = 'Smith')
