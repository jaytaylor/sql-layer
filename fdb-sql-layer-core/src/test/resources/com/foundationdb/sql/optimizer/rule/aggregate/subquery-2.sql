SELECT (SELECT name FROM customers WHERE customers.cid = orders.cid), COUNT(*)
  FROM orders
  GROUP BY cid
