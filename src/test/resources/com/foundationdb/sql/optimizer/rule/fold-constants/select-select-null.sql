SELECT name, (SELECT order_date = DATE(DATE(NULL)) FROM orders
               WHERE customers.cid = orders.cid)
  FROM customers
