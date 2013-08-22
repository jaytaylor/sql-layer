SELECT name, order_date, sku
  FROM customers 
 INNER JOIN orders ON customers.cid = orders.cid
 INNER JOIN items ON orders.oid = items.oid AND 1=0
