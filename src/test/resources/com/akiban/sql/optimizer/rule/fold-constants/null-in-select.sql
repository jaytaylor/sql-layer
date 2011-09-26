SELECT name
  FROM customers
 WHERE F(NULL) IN (SELECT order_date FROM orders WHERE customers.cid = orders.cid)
