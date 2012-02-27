SELECT customers.name,order_date,sku,quan 
  FROM customers
 RIGHT OUTER JOIN (orders INNER JOIN items ON orders.oid = items.oid)
    ON customers.cid = orders.cid
 WHERE order_date > '2011-01-01'
