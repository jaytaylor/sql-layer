SELECT name, order_date
  FROM customers INNER JOIN orders ON customers.cid = orders.cid
 WHERE (LCASE(NULL) AND LCASE(name)) OR (DATE(order_date) AND DATE(NULL))
