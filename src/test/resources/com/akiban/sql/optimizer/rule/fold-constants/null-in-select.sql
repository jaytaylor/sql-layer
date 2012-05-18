SELECT name
  FROM customers
 WHERE DATE(NULL) IN (SELECT order_date FROM orders WHERE customers.cid = orders.cid)
