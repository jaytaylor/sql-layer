SELECT name, order_date
  FROM customers INNER JOIN orders ON customers.cid = orders.cid
 WHERE name BETWEEN 'A' AND 'B'
   AND order_date BETWEEN '2010-01-01' AND '2019-12-31'