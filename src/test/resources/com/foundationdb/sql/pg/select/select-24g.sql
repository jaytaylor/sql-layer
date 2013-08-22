SELECT customers.name,order_date,sku,quan 
  FROM items
 INNER JOIN orders ON orders.oid = items.oid
  LEFT OUTER JOIN customers ON customers.cid = orders.cid
 WHERE order_date < '2011-03-01'
ORDER BY order_date, name
