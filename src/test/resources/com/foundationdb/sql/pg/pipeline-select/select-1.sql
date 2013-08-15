SELECT name, order_date, sku, quan 
  FROM customers INNER JOIN orders ON customers.cid = orders.cid LEFT JOIN items ON orders.oid = items.oid
 WHERE order_date > '2011-03-01'