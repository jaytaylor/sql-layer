SELECT *
 FROM customers INNER JOIN orders ON customers.cid = orders.cid
 WHERE UPPER(name) = 'SMITH'
   AND (DATE(order_date) = '2011-01-01' OR DATE(order_date) = '2012-01-01')