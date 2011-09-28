SELECT name, order_date, quan, sku 
  FROM customers 
 INNER JOIN orders ON customers.cid = orders.cid
 INNER JOIN items ON orders.oid = items.oid 
 WHERE order_date > '2011-01-01'
   AND quan > 100

