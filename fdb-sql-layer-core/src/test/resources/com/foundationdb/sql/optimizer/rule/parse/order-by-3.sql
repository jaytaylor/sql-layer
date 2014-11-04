SELECT customers.name, order_date 
  FROM customers LEFT OUTER JOIN orders ON customers.cid = orders.cid
ORDER BY 'constant'
