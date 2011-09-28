SELECT name, order_date
  FROM customers INNER JOIN orders ON customers.cid = orders.cid
 WHERE order_date > '2011-01-01' AND order_date <> '2011-04-01'
