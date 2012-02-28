SELECT customers.name,order_date,sku,quan 
  FROM items, orders LEFT JOIN customers ON customers.cid = orders.cid
 WHERE items.oid = orders.oid
   AND order_date > '2011-01-01'
