SELECT COUNT(*)
  FROM customers INNER JOIN orders on customers.cid = orders.cid
