SELECT orders.order_date, MAX(quan), MIN(quan) 
  FROM items INNER JOIN orders ON orders.oid = items.oid
 GROUP BY orders.order_date
