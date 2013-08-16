SELECT name, state, (SELECT MAX(order_date) FROM orders 
                      WHERE orders.cid = customers.cid) 
  FROM customers, addresses
 WHERE addresses.cid = customers.cid
