SELECT name, order_date
  FROM customers INNER JOIN orders ON customers.cid = orders.cid
 WHERE (F(NULL) AND G(name)) OR (F(order_date) AND G(NULL))
