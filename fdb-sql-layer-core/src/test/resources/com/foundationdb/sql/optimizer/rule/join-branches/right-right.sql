SELECT customers.name,order_date,sku,quan 
  FROM customers
  RIGHT JOIN orders ON customers.cid = orders.cid
  RIGHT JOIN items ON orders.oid = items.oid
 WHERE sku > '1'
