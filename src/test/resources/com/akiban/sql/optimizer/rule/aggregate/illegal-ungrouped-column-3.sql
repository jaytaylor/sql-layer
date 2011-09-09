SELECT orders.order_date, SUM(quan)
  FROM items INNER JOIN orders ON orders.oid = items.oid
 GROUP BY orders.order_date
 HAVING orders.oid != 100
