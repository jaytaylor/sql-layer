SELECT customers.name, (SELECT MAX(order_date) FROM orders 
                         WHERE customers.cid = orders.cid) AS max_date
  FROM customers ORDER BY name