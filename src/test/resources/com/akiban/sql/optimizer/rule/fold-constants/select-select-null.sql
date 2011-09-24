SELECT name, (SELECT order_date = DATE(G(NULL)) FROM orders
               WHERE customers.cid = orders.cid)
  FROM customers
