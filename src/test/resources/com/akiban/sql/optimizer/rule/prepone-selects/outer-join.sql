SELECT * FROM customers 
 RIGHT JOIN orders ON customers.cid = orders.cid 
  LEFT JOIN items ON orders.oid = items.oid
 WHERE MONTH(orders.order_date) = 1
   AND customers.name IS NULL
   AND items.sku IS NULL
