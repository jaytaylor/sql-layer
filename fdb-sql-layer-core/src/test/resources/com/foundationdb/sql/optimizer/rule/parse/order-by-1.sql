SELECT customers.name, order_date 
  FROM customers LEFT OUTER JOIN orders ON customers.cid = orders.cid
ORDER BY order_date DESC, name
LIMIT 1,10
